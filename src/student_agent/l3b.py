"""Evidence-based L3B coordination and independent verification."""

from __future__ import annotations

import copy
import json
from datetime import UTC
from decimal import Decimal
from typing import Any

import httpx2
from jsonschema import Draft202012Validator

from . import OUTPUT_SCHEMA_VERSION
from .contracts import Contracts
from .workflow import (
    Evidence,
    _classify,
    _collect_ids,
    _date,
    _dicts,
    _domain_data,
    _first_text,
    _issue_domains,
    _money,
    _norm,
    _resolution,
    _root_cause,
    _shipment_issue,
    _totals,
    _valid_payment_events,
    _valid_refund_events,
)


def timestamp(value: Any) -> float | None:
    parsed = _date(value)
    if parsed is None:
        return None
    return parsed.replace(tzinfo=UTC).timestamp() if parsed.tzinfo is None else parsed.timestamp()


def rows(value: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in _dicts(value)]


def resolve(case: dict, evidence: list[Evidence]) -> tuple[dict, dict, dict | None]:
    candidates = list(dict.fromkeys(case.get("candidate_order_ids", [])))
    claimed = case.get("customer_request", {}).get("claimed_order_id")
    if claimed and claimed not in candidates:
        candidates.append(claimed)
    customer = _domain_data(evidence, "customer")
    history = [row for value in customer for row in rows(value) if "order_id" in row]
    direct = [
        row
        for value in _domain_data(evidence, "order")
        for row in rows(value)
        if "order_id" in row and "order_purchase_timestamp" in row
    ]
    opened = timestamp(case.get("opened_at"))
    eligible = [
        row
        for row in history + direct
        if row.get("order_id") in candidates
        and (
            opened is None
            or (timestamp(row.get("order_purchase_timestamp")) or float("inf")) <= opened
        )
    ]
    order_ids = list(dict.fromkeys(row["order_id"] for row in eligible))
    selected = None
    if len(order_ids) == 1:
        versions = [row for row in eligible if row["order_id"] == order_ids[0]]
        newest = max(timestamp(row.get("order_purchase_timestamp")) or 0 for row in versions)
        matches = [
            row
            for row in versions
            if (timestamp(row.get("order_purchase_timestamp")) or 0) == newest
        ]
        unique = {json.dumps(row, sort_keys=True): row for row in matches}
        if len(unique) == 1:
            selected = next(iter(unique.values()))
    status = "resolved" if selected else ("ambiguous" if eligible else "not_found")
    related = list(dict.fromkeys(row["order_id"] for row in history))[:20]
    context = {
        "customer_unique_id": _first_text(customer, "customer_unique_id"),
        "related_order_ids": related,
    }
    resolution = {
        "status": status,
        "resolved_order_ids": [selected["order_id"]] if selected else [],
        "rejected_candidates": [
            item for item in candidates if item not in related and item not in order_ids
        ][:20]
        if history
        else [],
        "confidence": 0.92 if selected and history else (0.75 if selected else 0.25),
    }
    return resolution, context, selected


def normalize(evidence: list[Evidence], selected: dict) -> tuple[list[Evidence], list[dict]]:
    """Select temporally matching records without changing their original references."""
    purchase = timestamp(selected.get("order_purchase_timestamp"))
    order_id = selected["order_id"]
    conflicts = []
    result = []
    for record in evidence:
        data = copy.deepcopy(record.data)
        if record.domain == "order":
            if isinstance(data, dict) and any(
                data.get(key) != selected.get(key)
                for key in (
                    "order_status",
                    "order_purchase_timestamp",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date",
                )
            ):
                conflicts.append(
                    {
                        "field": "order_snapshot",
                        "sources": [
                            record.tool,
                            next(
                                (e.tool for e in evidence if e.domain == "customer"),
                                "customer_history",
                            ),
                        ],
                        "selected_source": next(
                            (e.tool for e in evidence if e.domain == "customer"), None
                        ),
                        "resolution_code": "MATCH_CUSTOMER_AND_CASE_TIME",
                    }
                )
            data = selected
        elif record.domain == "item" and isinstance(data, list):
            groups: dict[str, list[dict]] = {}
            for row in data:
                if row.get("order_id", order_id) == order_id:
                    groups.setdefault(str(row.get("order_item_id", row.get("item_id"))), []).append(
                        row
                    )
            data = []
            for group in groups.values():

                def distance(row: dict) -> float:
                    limit = timestamp(row.get("shipping_limit_date"))
                    return (
                        abs(limit - purchase) if limit is not None and purchase is not None else 0
                    )

                data.append(min(group, key=distance))
        elif record.domain in {"payment", "refund", "shipment"} and isinstance(data, dict):
            events = data.get("events", [])
            if purchase is not None:
                # Stop at the next distinct purchase for this identifier, if present.
                future = [
                    timestamp(row.get("order_purchase_timestamp"))
                    for e in evidence
                    if e.domain == "customer"
                    for row in rows(e.data)
                    if row.get("order_id") == order_id
                ]
                end = min(
                    (value for value in future if value is not None and value > purchase),
                    default=float("inf"),
                )
                data["events"] = [
                    row
                    for row in events
                    if row.get("order_id", order_id) == order_id
                    and purchase <= (timestamp(row.get("event_at")) or 0) < end
                ]
            if record.domain == "payment":
                # Summary payments lack timestamps; use dated ledger events for accounting.
                data.pop("payments", None)
            if record.domain == "shipment":
                mapping = {
                    "order_status": "order_status",
                    "delivered_carrier_at": "order_delivered_carrier_date",
                    "delivered_customer_at": "order_delivered_customer_date",
                    "estimated_delivery_at": "order_estimated_delivery_date",
                }
                different = any(
                    data.get(key) != selected.get(source) for key, source in mapping.items()
                )
                if different:
                    conflicts.append(
                        {
                            "field": "shipment_timeline",
                            "sources": [
                                record.tool,
                                next(
                                    (e.tool for e in evidence if e.domain == "customer"),
                                    "get_order",
                                ),
                            ],
                            "selected_source": next(
                                (e.tool for e in evidence if e.domain == "customer"), "get_order"
                            ),
                            "resolution_code": "MATCH_RESOLVED_ORDER_TIMELINE",
                        }
                    )
                data.update({key: selected.get(source) for key, source in mapping.items()})
                data["handed_to_carrier_at"] = selected.get("order_delivered_carrier_date")
        result.append(Evidence(record.tool, record.domain, record.ref, data))
    # Customer history is the authoritative selected snapshot when direct lookup is unavailable.
    if not any(e.domain == "order" for e in result):
        source = next((e for e in evidence if e.domain == "customer"), None)
        if source:
            result.append(Evidence(source.tool, "order", source.ref, selected))
    return result, conflicts[:5]


def shipment_analysis(evidence: list[Evidence], selected: dict | None) -> dict:
    verdict = "insufficient_evidence"
    late_sellers = []
    actual = timestamp((selected or {}).get("order_delivered_customer_date"))
    promised = timestamp((selected or {}).get("order_estimated_delivery_date"))
    handoff = timestamp((selected or {}).get("order_delivered_carrier_date"))
    shipment = _domain_data(evidence, "shipment")
    if selected and shipment:
        issue = _shipment_issue(evidence)
        verdict = {
            "late_delivery_seller": "seller_delay",
            "late_delivery_logistics": "logistics_delay",
        }.get(issue, verdict)
        events = [row for value in shipment for row in rows(value) if "event_type" in row]
        if any(_norm(str(row["event_type"])) in {"lost", "shipment_lost"} for row in events):
            verdict = "lost"
        elif any(
            _norm(str(row["event_type"])) in {"returned", "returned_to_sender"} for row in events
        ):
            verdict = "returned"
        elif not issue and actual is not None and promised is not None and actual <= promised:
            verdict = "on_time"
        if verdict == "seller_delay":
            for value in _domain_data(evidence, "item"):
                for row in rows(value):
                    limit = timestamp(row.get("shipping_limit_date"))
                    if (
                        handoff is not None
                        and limit is not None
                        and handoff > limit
                        and (seller := row.get("seller_id"))
                    ):
                        late_sellers.append(seller)
    return {
        "verdict": verdict,
        "late_seller_ids": list(dict.fromkeys(late_sellers))[:20],
        "timeline_complete": bool(shipment)
        and all(value is not None for value in (actual, promised, handoff)),
    }


def payment_analysis(evidence: list[Evidence]) -> dict:
    expected, captured, refunded = _totals(evidence)
    refund_available = any(e.domain == "refund" for e in evidence)
    events = _valid_payment_events(evidence)
    refund_events = _valid_refund_events(evidence)
    statuses = {_norm(str(row.get("status", ""))) for row in refund_events}
    verdict = "insufficient_evidence"
    if "failed" in statuses:
        verdict = "refund_failed"
    elif "pending" in statuses:
        verdict = "refund_pending"
    elif captured is not None and captured > 0 and refunded >= captured:
        verdict = "refunded"
    elif any(_norm(str(row.get("event_type", ""))) == "reconciliation_mismatch" for row in events):
        verdict = "capture_mismatch"
    elif captured is not None and expected is not None:
        captures = [
            row for row in events if row.get("event_type") in {"captured", "capture", "paid"}
        ]
        if captured > expected + Decimal("0.01") and len(captures) > 1:
            verdict = "duplicate_capture"
        elif abs(captured - expected) > Decimal("0.01"):
            verdict = "capture_mismatch"
        else:
            verdict = "reconciled"
    return {
        "verdict": verdict,
        "captured_total_brl": _money(captured) if captured is not None else None,
        "refunded_total_brl": _money(refunded) if refund_available else None,
        "refundable_total_brl": _money(max(Decimal(), captured - refunded))
        if captured is not None and refund_available
        else None,
    }


def assess(case: dict, raw: list[Evidence]) -> dict:
    entity, customer, selected = resolve(case, raw)
    evidence, conflicts = normalize(raw, selected) if selected else (raw, [])
    issue, confidence, expected, captured, refunded = _classify(evidence)
    shipment = shipment_analysis(evidence, selected)
    payment = (
        payment_analysis(evidence)
        if selected
        else {
            "verdict": "insufficient_evidence",
            "captured_total_brl": None,
            "refunded_total_brl": None,
            "refundable_total_brl": None,
        }
    )
    domains = {e.domain for e in raw}
    if not selected or not {"payment", "shipment", "policy"}.issubset(domains):
        issue, confidence = "insufficient_evidence", 0.25
    confidence = min(confidence, entity["confidence"])
    if "refund" not in domains:
        confidence = min(confidence, 0.7)
    ids = _collect_ids([e.data for e in evidence if e.domain not in {"customer", "policy"}])
    ids["order_ids"] = entity["resolved_order_ids"]
    status, refund, lines, actions = _resolution(issue, evidence, ids, expected, captured, refunded)
    if issue == "insufficient_evidence":
        status, refund, lines, actions = (
            "needs_investigation",
            Decimal(),
            [],
            ["REQUEST_ADDITIONAL_EVIDENCE"],
        )
    if status == "no_action":
        refund, lines = Decimal(), []
    if captured is not None and refund > max(Decimal(), captured - refunded):
        refund = max(Decimal(), captured - refunded)
        lines = (
            [
                {
                    "reason_code": issue.upper(),
                    "amount_brl": _money(refund),
                    "entity_id": (ids["order_ids"] or [None])[0],
                }
            ]
            if refund
            else []
        )
    secondary = []
    independently_found = {
        "seller_delay": "late_delivery_seller",
        "logistics_delay": "late_delivery_logistics",
        "capture_mismatch": "payment_mismatch",
        "duplicate_capture": "duplicate_charge",
        "refund_pending": "refund_pending",
        "refund_failed": "refund_failed",
    }
    for finding in (shipment["verdict"], payment["verdict"]):
        other = independently_found.get(finding)
        if other and other != issue and other not in secondary:
            secondary.append(other)
    refs = list(dict.fromkeys(e.ref for e in raw))
    root_cause = _root_cause(issue, evidence, ids)
    # Policy may contain example party IDs; attribute only to parties in this order's evidence.
    for party in root_cause["responsible_parties"]:
        if party["party_type"] == "seller" and party["party_id"] not in ids["seller_ids"]:
            party["party_id"] = (shipment["late_seller_ids"] or ids["seller_ids"] or [None])[0]
    output = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "case_id": case["case_id"],
        "assessment": {
            "primary_issue": issue,
            "secondary_issues": secondary,
            "case_status": status,
            "confidence": confidence,
        },
        "affected_entities": ids,
        "entity_resolution": entity,
        "customer_context": customer,
        "shipment_analysis": shipment,
        "payment_analysis": payment,
        "root_cause_analysis": root_cause,
        "evidence_refs": refs,
        "data_conflicts": conflicts,
        "financial_resolution": {
            "currency": "BRL",
            "recommended_refund_brl": _money(refund),
            "refund_lines": lines,
        },
        "resolution_actions": actions,
    }
    claims = []
    for claim in case.get("customer_request", {}).get("claims", [])[:5]:
        topic = claim.get("topic", "")
        verdict = "unsupported"
        if issue == "insufficient_evidence":
            verdict = "insufficient_evidence"
        elif topic in [issue, *secondary] and topic != "unsupported_claim":
            verdict = "supported"
        elif topic == "requested_full_refund":
            if captured is None:
                verdict = "insufficient_evidence"
            elif refund > 0:
                verdict = "supported" if refund >= captured - refunded else "partially_supported"
        claim_domains = (
            _issue_domains(topic)
            if topic != "requested_full_refund"
            else {"order", "payment", "refund", "policy"}
        )
        claims.append(
            {
                "claim_id": claim["claim_id"],
                "verdict": verdict,
                "confidence": confidence,
                "evidence_refs": [e.ref for e in raw if e.domain in claim_domains],
            }
        )
    if claims:
        output["claim_assessments"] = claims
    return output


def verify(case: dict, output: dict, evidence: list[Evidence], contracts: Contracts) -> None:
    contracts.validate_output(output, "verifier output")
    if output["case_id"] != case["case_id"]:
        raise ValueError("Verifier: case ID mismatch")
    refs = {record.ref for record in evidence}
    used = set(output["evidence_refs"])
    used.update(
        ref for claim in output.get("claim_assessments", []) for ref in claim["evidence_refs"]
    )
    if not used <= refs:
        raise ValueError("Verifier: evidence was not consumed in this case")
    financial = output["financial_resolution"]
    total = sum((Decimal(str(row["amount_brl"])) for row in financial["refund_lines"]), Decimal())
    if total != Decimal(str(financial["recommended_refund_brl"])):
        raise ValueError("Verifier: refund lines do not add up")
    if output["assessment"]["case_status"] == "no_action" and total:
        raise ValueError("Verifier: no-action case cannot recommend a refund")
    entity = output["entity_resolution"]
    if set(entity["resolved_order_ids"]) & set(entity["rejected_candidates"]):
        raise ValueError("Verifier: order both resolved and rejected")
    if output["affected_entities"]["order_ids"] != entity["resolved_order_ids"]:
        raise ValueError("Verifier: affected order differs from resolved order")


async def solve(case: dict, gateway: Any, trace: Any) -> dict:
    tools = await gateway.describe_tools()
    evidence: list[Evidence] = []
    cache: dict[str, Evidence | None] = {}
    case_id = case["case_id"]
    failures = 0

    async def fetch(tool: str, actor: str, **arguments: Any) -> Evidence | None:
        nonlocal failures
        if tool not in tools:
            return None
        payload = {"case_id": case_id, **arguments}
        Draft202012Validator(tools[tool]).validate(payload)
        key = json.dumps([tool, payload], sort_keys=True)
        if key in cache:
            return cache[key]
        for attempt in range(2):
            try:
                response = await gateway.call(tool, case_id=case_id, **arguments)
                record = Evidence(
                    tool, response["domain"], response["evidence_ref"], response["data"]
                )
                evidence.append(record)
                trace.emit(
                    case_id=case_id,
                    event_type="tool_result_consumed",
                    actor=actor,
                    tool_name=tool,
                    evidence_refs=[record.ref],
                )
                cache[key] = record
                return record
            except (httpx2.TimeoutException, httpx2.ConnectError, TimeoutError, ConnectionError):
                if attempt == 0:
                    continue
            except (ValueError, RuntimeError):
                pass
            failures += 1
            cache[key] = None
            return None
        return None

    def assign(actor: str, code: str) -> None:
        trace.emit(
            case_id=case_id,
            event_type="task_assigned",
            actor="coordinator",
            target=actor,
            decision_code=code,
        )

    def handoff(actor: str, start: int, target: str = "verifier") -> None:
        trace.emit(
            case_id=case_id,
            event_type="handoff",
            actor=actor,
            target=target,
            decision_code="EVIDENCE_READY",
            evidence_refs=[e.ref for e in evidence[start:]][:20],
        )

    assign("entity-agent", "RESOLVE_ORDER_AND_CUSTOMER")
    hint = case.get("customer_unique_id_hint")
    if hint:
        await fetch("get_customer_history", "entity-agent", customer_unique_id=hint)
    entity, _, selected = resolve(case, evidence)
    claimed = case.get("customer_request", {}).get("claimed_order_id")
    candidates = list(dict.fromkeys([claimed, *case.get("candidate_order_ids", [])]))
    if selected:
        await fetch("get_order", "entity-agent", order_id=selected["order_id"])
    else:
        for candidate in candidates[:5]:
            if candidate:
                await fetch("get_order", "entity-agent", order_id=candidate)
    entity, _, selected = resolve(case, evidence)
    handoff("entity-agent", 0, "coordinator")
    if selected:
        order_id = selected["order_id"]
        for actor, code, names in (
            (
                "order-product-agent",
                "CHECK_ITEMS_AND_PRODUCTS",
                ["get_order_items", "get_product_context"],
            ),
            ("shipment-agent", "CHECK_SHIPMENT_TIMELINE", ["get_shipment_summary"]),
            (
                "payment-agent",
                "RECONCILE_PAYMENTS_AND_REFUNDS",
                ["get_payment_timeline", "get_refund_timeline"],
            ),
        ):
            assign(actor, code)
            start = len(evidence)
            for name in names:
                await fetch(name, actor, order_id=order_id)
            handoff(actor, start)
    assign("policy-agent", "CHECK_APPLICABLE_POLICY")
    start = len(evidence)
    if version := case.get("policy_version"):
        await fetch("get_policy", "policy-agent", policy_version=version)
    handoff("policy-agent", start)
    output = assess(case, evidence)
    trace.emit(
        case_id=case_id,
        event_type="policy_decided",
        actor="policy-agent",
        target="verifier",
        decision_code=output["assessment"]["primary_issue"].upper(),
        evidence_refs=[e.ref for e in evidence if e.domain == "policy"],
    )
    assign("verifier", "VERIFY_SCHEMA_SCOPE_AND_ACCOUNTING")
    verify(case, output, evidence, trace.contracts)
    trace.emit(
        case_id=case_id,
        event_type="verification_completed",
        actor="verifier",
        target="coordinator",
        decision_code="INVARIANTS_PASSED",
        evidence_refs=output["evidence_refs"][:20],
        attributes={
            "evidence_count": len(evidence),
            "failed_calls": failures,
            "entity_status": entity["status"],
        },
    )
    return output
