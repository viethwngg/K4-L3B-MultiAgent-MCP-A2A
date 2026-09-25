from __future__ import annotations

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx2
import pytest

from student_agent.cases import CaseSet
from student_agent.contracts import Contracts
from student_agent.l3b import assess, verify
from student_agent.submission import validate_artifacts
from student_agent.trace import TraceWriter
from student_agent.workflow import Evidence, solve_case


def fixture_data():
    case = {
        "case_id": "CASE_001",
        "opened_at": "2020-01-08T00:00:00Z",
        "customer_request": {"claimed_order_id": "order-a", "claims": []},
        "candidate_order_ids": ["order-a", "other-order"],
        "customer_unique_id_hint": "customer-a",
        "policy_version": "v2",
    }
    order = {
        "order_id": "order-a",
        "order_status": "delivered",
        "order_purchase_timestamp": "2020-01-01T00:00:00Z",
        "order_approved_at": "2020-01-01T01:00:00Z",
        "order_delivered_carrier_date": "2020-01-02T00:00:00Z",
        "order_delivered_customer_date": "2020-01-09T00:00:00Z",
        "order_estimated_delivery_date": "2020-01-07T00:00:00Z",
    }
    future = {
        **order,
        "order_purchase_timestamp": "2021-01-01T00:00:00Z",
        "order_status": "canceled",
    }
    data = [
        (
            "get_customer_history",
            "customer",
            {"customer_unique_id": "customer-a", "orders": [future, order]},
        ),
        ("get_order", "order", future),
        (
            "get_order_items",
            "item",
            [
                {
                    "order_id": "order-a",
                    "order_item_id": "item-a",
                    "seller_id": "seller-a",
                    "price": "80",
                    "freight_value": "10",
                    "shipping_limit_date": "2020-01-03T00:00:00Z",
                }
            ],
        ),
        (
            "get_shipment_summary",
            "shipment",
            {
                "order_status": "canceled",
                "events": [
                    {
                        "event_type": "delivered_late",
                        "event_at": "2020-01-09T00:00:00Z",
                        "actor": "logistics_provider",
                    }
                ],
            },
        ),
        (
            "get_payment_timeline",
            "payment",
            {
                "events": [
                    {
                        "event_type": "captured",
                        "event_at": "2020-01-01T01:00:00Z",
                        "amount_brl": "90",
                    },
                    {
                        "event_type": "captured",
                        "event_at": "2021-01-01T01:00:00Z",
                        "amount_brl": "150",
                    },
                ]
            },
        ),
        ("get_refund_timeline", "refund", {"events": []}),
        ("get_product_context", "product", [{"product_id": "product-a"}]),
        (
            "get_policy",
            "policy",
            {
                "rules": {
                    "late_delivery_logistics": {
                        "case_status": "action_required",
                        "refund_brl": 10,
                        "recommended_action": "refund_freight",
                    }
                }
            },
        ),
    ]
    evidence = [
        Evidence(tool, domain, f"ev_{index:024d}", value)
        for index, (tool, domain, value) in enumerate(data)
    ]
    return case, evidence


def contracts():
    return Contracts(Path(__file__).resolve().parents[1] / "contracts/schemas")


def test_temporal_resolution_and_accounting():
    case, evidence = fixture_data()
    output = assess(case, evidence)
    verify(case, output, evidence, contracts())
    assert output["assessment"]["primary_issue"] == "late_delivery_logistics"
    assert output["entity_resolution"]["rejected_candidates"] == ["other-order"]
    assert output["affected_entities"]["order_ids"] == ["order-a"]
    assert output["payment_analysis"]["captured_total_brl"] == 90
    assert output["financial_resolution"]["recommended_refund_brl"] == 10
    assert output["data_conflicts"][0]["selected_source"] == "get_customer_history"


def test_split_payment_is_not_duplicate():
    case, evidence = fixture_data()
    evidence = copy.deepcopy(evidence)
    payment = next(e.data for e in evidence if e.domain == "payment")
    payment["events"] = [
        {"event_type": "captured", "event_at": "2020-01-01T01:00:00Z", "amount_brl": value}
        for value in ("30", "60")
    ]
    output = assess(case, evidence)
    assert output["payment_analysis"]["verdict"] == "reconciled"
    assert output["payment_analysis"]["captured_total_brl"] == 90


def test_missing_refund_is_unknown_not_zero():
    case, evidence = fixture_data()
    output = assess(case, [e for e in evidence if e.domain != "refund"])
    assert output["payment_analysis"]["refunded_total_brl"] is None
    assert output["payment_analysis"]["refundable_total_brl"] is None
    assert output["assessment"]["confidence"] <= 0.7


def test_verifier_rejects_unconsumed_evidence():
    case, evidence = fixture_data()
    output = assess(case, evidence)
    output["evidence_refs"].append("ev_" + "x" * 24)
    with pytest.raises(ValueError, match="not consumed"):
        verify(case, output, evidence, contracts())


def test_ambiguous_entity_does_not_select_claimed_order():
    case, evidence = fixture_data()
    evidence = copy.deepcopy(evidence)
    history = evidence[0].data["orders"]
    history.append({**history[1], "order_id": "other-order"})
    output = assess(case, evidence)
    assert output["entity_resolution"]["status"] == "ambiguous"
    assert output["assessment"]["primary_issue"] == "insufficient_evidence"
    assert output["financial_resolution"]["recommended_refund_brl"] == 0


def test_workflow_scopes_calls_and_trace(tmp_path):
    case, evidence = fixture_data()

    class Gateway:
        def __init__(self):
            self.calls = []

        async def describe_tools(self):
            return {
                e.tool: {
                    "type": "object",
                    "properties": {
                        "case_id": {"type": "string"},
                        (
                            "customer_unique_id"
                            if e.domain == "customer"
                            else "policy_version"
                            if e.domain == "policy"
                            else "order_id"
                        ): {"type": "string"},
                    },
                    "required": ["case_id"],
                }
                for e in evidence
            }

        async def call(self, tool, *, case_id, **arguments):
            self.calls.append((tool, case_id, arguments))
            record = next(e for e in evidence if e.tool == tool)
            return {"domain": record.domain, "evidence_ref": record.ref, "data": record.data}

    gateway = Gateway()
    trace = TraceWriter(tmp_path / "trace.jsonl", contracts())
    output = asyncio.run(solve_case(case, gateway, trace))
    assert len(gateway.calls) == 7
    assert all(call[1] == case["case_id"] for call in gateway.calls)
    events = [json.loads(line) for line in trace.path.read_text().splitlines()]
    consumed = {
        ref
        for event in events
        if event["event_type"] == "tool_result_consumed"
        for ref in event["evidence_refs"]
    }
    assert set(output["evidence_refs"]) == consumed
    assert events[-1]["event_type"] == "verification_completed"


@pytest.mark.parametrize("fault", [None, "foreign_reference", "incomplete_trace"])
def test_artifact_provenance_checks(tmp_path, fault):
    case, evidence = fixture_data()
    case_id = case["case_id"]
    output = assess(case, evidence)
    if fault == "foreign_reference":
        output["evidence_refs"].append("ev_" + "z" * 24)
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    (output_dir / f"{case_id}.json").write_text(json.dumps(output), encoding="utf-8")
    trace = TraceWriter(tmp_path / "traces/trace.jsonl", contracts())
    trace.emit(case_id=case_id, event_type="case_received", actor="coordinator")
    trace.emit(
        case_id=case_id,
        event_type="tool_result_consumed",
        actor="specialist",
        evidence_refs=[e.ref for e in evidence],
    )
    if fault != "incomplete_trace":
        trace.emit(case_id=case_id, event_type="verification_completed", actor="verifier")
    trace.emit(case_id=case_id, event_type="case_finalized", actor="coordinator")
    case_set = CaseSet("test-v1", "l3b", (case_id,), {case_id: case})
    if fault:
        with pytest.raises(ValueError, match="not consumed|incomplete verified"):
            validate_artifacts(tmp_path, case_set, contracts())
    else:
        outputs, lines = validate_artifacts(tmp_path, case_set, contracts())
        assert set(outputs) == {case_id}
        assert len(lines) == 4


@pytest.mark.parametrize("interrupt", [False, True])
def test_cli_reconnect_and_resume_preserve_verified_cases(tmp_path, monkeypatch, interrupt):
    from student_agent import cli

    case, evidence = fixture_data()
    case_set = CaseSet("test-v1", "l3b", (case["case_id"],), {case["case_id"]: case})
    monkeypatch.setattr(cli, "load_case_set", lambda root: case_set)
    monkeypatch.setattr(cli, "Contracts", lambda root: contracts())
    monkeypatch.setattr(
        cli.Settings,
        "load",
        lambda root: SimpleNamespace(
            mcp_endpoint="https://example.invalid/mcp", team_api_key="test-only"
        ),
    )
    calls = []

    class Gateway:
        async def list_tools(self):
            return [e.tool for e in evidence]

    @asynccontextmanager
    async def connect(*args):
        calls.append("connect")
        yield Gateway()

    async def solve(case, gateway, trace):
        trace.emit(
            case_id=case["case_id"],
            event_type="tool_result_consumed",
            actor="specialist",
            evidence_refs=[e.ref for e in evidence],
        )
        if interrupt and len(calls) == 1:
            raise httpx2.ConnectError("simulated transport interruption")
        output = assess(case, evidence)
        trace.emit(case_id=case["case_id"], event_type="verification_completed", actor="verifier")
        return output

    monkeypatch.setattr(cli, "connect_gateway", connect)
    monkeypatch.setattr(cli, "solve_case", solve)
    asyncio.run(cli._run(tmp_path, workers=1))
    assert len(calls) == (2 if interrupt else 1)
    before = (tmp_path / "traces/trace.jsonl").read_bytes()
    asyncio.run(cli._run(tmp_path, resume=True, workers=1))
    assert len(calls) == (2 if interrupt else 1)
    assert (tmp_path / "traces/trace.jsonl").read_bytes() == before
    _, lines = validate_artifacts(tmp_path, case_set, contracts())
    assert len(lines) == 4
