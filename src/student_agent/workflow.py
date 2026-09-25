from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .mcp_gateway import EvidenceGateway
from .trace import TraceWriter

ACTORS = {
    "order": "order-item-agent",
    "item": "order-item-agent",
    "payment": "payment-agent",
    "refund": "payment-agent",
    "shipment": "shipment-agent",
    "seller": "order-item-agent",
    "policy": "policy-agent",
    "customer": "coordinator",
    "product": "order-item-agent",
}
DOMAINS = tuple(ACTORS)


@dataclass(frozen=True)
class Evidence:
    tool: str
    domain: str
    ref: str
    data: Any


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _walk(value: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            yield _norm(str(key)), child
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _dicts(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        for child in value.values():
            yield from _dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _dicts(child)


def _values(value: Any, *names: str) -> list[Any]:
    wanted = {_norm(name) for name in names}
    return [child for key, child in _walk(value) if key in wanted]


def _strings(value: Any) -> list[str]:
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    if isinstance(value, (list, tuple, set)):
        return [text for item in value for text in _strings(item)]
    return []


def _first_text(value: Any, *names: str) -> str | None:
    for candidate in _values(value, *names):
        texts = _strings(candidate)
        if texts:
            return texts[0]
    return None


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() and number >= 0 else None


def _first_money(value: Any, *names: str) -> Decimal | None:
    for candidate in _values(value, *names):
        number = _decimal(candidate)
        if number is not None:
            return number
    return None


def _money(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01")))


def _date(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def _truth(value: Any, *names: str) -> bool:
    return any(
        candidate is True
        or (isinstance(candidate, str) and _norm(candidate) in {"true", "yes", "y", "1"})
        for candidate in _values(value, *names)
    )


def _collect_ids(value: Any) -> dict[str, list[str]]:
    aliases = {
        "order_ids": {"order_id", "order_ids", "claimed_order_id"},
        "item_ids": {"item_id", "item_ids", "order_item_id", "order_item_ids"},
        "seller_ids": {"seller_id", "seller_ids"},
        "payment_references": {
            "payment_reference",
            "payment_references",
            "payment_id",
            "payment_ids",
            "transaction_id",
            "transaction_ids",
            "charge_id",
            "charge_ids",
        },
        "shipment_ids": {
            "shipment_id",
            "shipment_ids",
            "tracking_id",
            "tracking_ids",
            "tracking_code",
            "tracking_codes",
        },
    }
    result = {name: [] for name in aliases}
    for key, child in _walk(value):
        for kind, keys in aliases.items():
            if key not in keys:
                continue
            candidates = _strings(child)
            if not candidates and isinstance(child, (int, float)) and not isinstance(child, bool):
                candidates = [str(child)]
            for candidate in candidates:
                if candidate not in result[kind] and len(result[kind]) < 20:
                    result[kind].append(candidate)
    return result


def _domain_data(evidence: list[Evidence], *domains: str) -> list[Any]:
    return [record.data for record in evidence if record.domain in set(domains)]


def _all_text(values: Iterable[Any]) -> str:
    return " ".join(
        _norm(child) for value in values for _, child in _walk(value) if isinstance(child, str)
    )


def _order_dates(evidence: list[Evidence]) -> tuple[datetime | None, datetime | None]:
    purchase = approved = None
    for data in _domain_data(evidence, "order"):
        purchase = purchase or _date(
            _first_text(data, "order_purchase_timestamp", "purchased_at", "created_at")
        )
        approved = approved or _date(
            _first_text(data, "order_approved_at", "approved_at", "paid_at")
        )
    return purchase, approved


def _event_rows(evidence: list[Evidence], domain: str) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for data in _domain_data(evidence, domain):
        for row in _dicts(data):
            keys = {_norm(str(key)) for key in row}
            if "event_type" in keys and row not in rows:
                rows.append(row)
    return rows


def _near(date: datetime | None, anchor: datetime | None, days: int = 7) -> bool:
    if date is None or anchor is None:
        return anchor is None
    return abs((date - anchor).total_seconds()) <= days * 86400


def _valid_payment_events(evidence: list[Evidence]) -> list[Mapping[str, Any]]:
    purchase, approved = _order_dates(evidence)
    anchor = approved or purchase
    return [
        row
        for row in _event_rows(evidence, "payment")
        if _near(_date(str(row.get("event_at", ""))), anchor)
    ]


def _valid_refund_events(evidence: list[Evidence]) -> list[Mapping[str, Any]]:
    purchase, _ = _order_dates(evidence)
    result = []
    for row in _event_rows(evidence, "refund"):
        event_at = _date(str(row.get("event_at", "")))
        if purchase is None or (event_at is not None and event_at >= purchase):
            result.append(row)
    return result


def _expected_item_total(evidence: list[Evidence]) -> Decimal | None:
    purchase, _ = _order_dates(evidence)
    candidates: dict[str, tuple[float, Decimal]] = {}
    for data in _domain_data(evidence, "item"):
        for index, row in enumerate(_dicts(data)):
            price = _decimal(row.get("price"))
            freight = _decimal(row.get("freight_value")) or Decimal()
            if price is None:
                continue
            item_id = str(row.get("order_item_id") or row.get("item_id") or index)
            limit = _date(str(row.get("shipping_limit_date", "")))
            distance = (
                abs((limit - purchase).total_seconds()) if limit and purchase else float(index)
            )
            current = candidates.get(item_id)
            if current is None or distance < current[0]:
                candidates[item_id] = (distance, price + freight)
    return sum((entry[1] for entry in candidates.values()), Decimal()) if candidates else None


def _totals(evidence: list[Evidence]) -> tuple[Decimal | None, Decimal | None, Decimal]:
    expected = next(
        (
            amount
            for data in _domain_data(evidence, "order", "item")
            if (
                amount := _first_money(
                    data,
                    "order_total_brl",
                    "order_total",
                    "total_order_value",
                    "grand_total",
                    "total_amount",
                    "total_value",
                    "payable_total_brl",
                )
            )
            is not None
        ),
        None,
    )
    expected = expected or _expected_item_total(evidence)
    captured_amounts = [
        amount
        for row in _valid_payment_events(evidence)
        if _norm(str(row.get("event_type", ""))) in {"captured", "capture", "paid"}
        and (amount := _decimal(row.get("amount_brl") or row.get("amount"))) is not None
    ]
    captured = sum(captured_amounts, Decimal()) if captured_amounts else None
    refunded_amounts = [
        amount
        for row in _valid_refund_events(evidence)
        if _norm(str(row.get("status", "")))
        in {"refunded", "completed", "succeeded", "processed", "success"}
        and (amount := _decimal(row.get("amount_brl") or row.get("amount"))) is not None
    ]
    refunded = sum(refunded_amounts, Decimal())
    return expected, captured, refunded


def _payment_count(evidence: list[Evidence]) -> int:
    return sum(
        _norm(str(row.get("event_type", ""))) in {"captured", "capture", "paid"}
        for row in _valid_payment_events(evidence)
    )


def _shipment_issue(evidence: list[Evidence]) -> str | None:
    data = _domain_data(evidence, "shipment", "order")
    actual = next(
        (
            parsed
            for value in data
            if (
                parsed := _date(
                    _first_text(
                        value,
                        "delivered_customer_at",
                        "delivered_at",
                        "actual_delivery_at",
                        "order_delivered_customer_date",
                    )
                )
            )
            is not None
        ),
        None,
    )
    for row in _event_rows(evidence, "shipment"):
        if _norm(str(row.get("event_type", ""))) not in {"delivered_late", "late_delivery"}:
            continue
        event_at = _date(str(row.get("event_at", "")))
        if actual is not None and event_at != actual:
            continue
        actor = _norm(str(row.get("actor", row.get("responsible_party", ""))))
        if actor == "seller":
            return "late_delivery_seller"
        if actor in {"logistics", "logistics_provider", "carrier"}:
            return "late_delivery_logistics"
    text = _all_text(data)
    if any(token in text for token in ("seller_delay", "seller_late", "late_seller")):
        return "late_delivery_seller"
    if any(token in text for token in ("logistics_delay", "carrier_delay", "late_logistics")):
        return "late_delivery_logistics"
    promised = handoff = due = None
    for value in data:
        actual = actual or _date(
            _first_text(
                value, "delivered_at", "actual_delivery_at", "order_delivered_customer_date"
            )
        )
        promised = promised or _date(
            _first_text(
                value,
                "estimated_delivery_at",
                "promised_delivery_at",
                "order_estimated_delivery_date",
            )
        )
        handoff = handoff or _date(
            _first_text(value, "handed_to_carrier_at", "shipped_at", "order_delivered_carrier_date")
        )
        due = due or _date(
            _first_text(value, "expected_handoff_at", "shipping_deadline", "shipping_limit_date")
        )
    if actual and promised and actual > promised:
        return (
            "late_delivery_seller"
            if handoff and due and handoff > due
            else "late_delivery_logistics"
        )
    return None


def _classify(
    evidence: list[Evidence],
) -> tuple[str, float, Decimal | None, Decimal | None, Decimal]:
    expected, captured, refunded = _totals(evidence)
    order_text = _all_text(_domain_data(evidence, "order", "item"))
    payment_text = _all_text(_domain_data(evidence, "payment", "refund"))
    all_data = [record.data for record in evidence]
    paid = captured is not None and captured > refunded
    refund_statuses = {_norm(str(row.get("status", ""))) for row in _valid_refund_events(evidence)}
    duplicate = (
        _truth(all_data, "is_duplicate", "duplicate_charge", "duplicate_capture")
        or any(
            token in payment_text
            for token in ("duplicate_charge", "duplicate_capture", "duplicated_charge")
        )
        or (
            expected is not None
            and captured is not None
            and _payment_count(evidence) > 1
            and captured > expected + Decimal("0.01")
        )
    )
    reconciliation_mismatch = any(
        _norm(str(row.get("event_type", ""))) == "reconciliation_mismatch"
        for row in _valid_payment_events(evidence)
    )
    balanced_split = (
        _payment_count(evidence) > 1
        and expected is not None
        and captured is not None
        and abs(expected - captured) <= Decimal("0.01")
    )
    if (
        any(
            token in order_text
            for token in ("canceled", "cancelled", "order_cancelled", "order_canceled")
        )
        and paid
    ):
        issue, confidence = "canceled_order_paid", 0.95
    elif any(token in order_text for token in ("unavailable", "out_of_stock", "stockout")) and paid:
        issue, confidence = "unavailable_order_paid", 0.95
    elif shipment := _shipment_issue(evidence):
        issue, confidence = shipment, 0.95
    elif reconciliation_mismatch:
        issue, confidence = "payment_mismatch", 0.94
    elif balanced_split:
        issue, confidence = "valid_split_payment", 0.92
    elif "failed" in refund_statuses or any(
        token in payment_text for token in ("refund_failed", "failed_refund")
    ):
        issue, confidence = "refund_failed", 0.95
    elif "pending" in refund_statuses or any(
        token in payment_text for token in ("refund_pending", "pending_refund", "refund_processing")
    ):
        issue, confidence = "refund_pending", 0.93
    elif duplicate:
        issue, confidence = "duplicate_charge", 0.96
    elif (
        expected is not None and captured is not None and abs(expected - captured) > Decimal("0.01")
    ):
        issue, confidence = "payment_mismatch", 0.82
    elif evidence:
        issue, confidence = "unsupported_claim", 0.78
    else:
        issue, confidence = "insufficient_evidence", 0.2
    return issue, confidence, expected, captured, refunded


def _issue_domains(issue: str) -> set[str]:
    if issue.startswith("late_delivery"):
        return {"order", "item", "shipment", "seller", "policy"}
    if issue in {"canceled_order_paid", "unavailable_order_paid"}:
        return {"order", "item", "product", "payment", "refund", "policy"}
    if issue in {"valid_split_payment", "payment_mismatch", "duplicate_charge"}:
        return {"order", "payment", "policy"}
    if issue.startswith("refund_"):
        return {"payment", "refund", "policy"}
    return set(DOMAINS)


def _policy_rule(evidence: list[Evidence], issue: str) -> Mapping[str, Any] | None:
    for data in _domain_data(evidence, "policy"):
        rules = data.get("rules") if isinstance(data, Mapping) else None
        if isinstance(rules, Mapping) and isinstance(rules.get(issue), Mapping):
            return rules[issue]
    return None


def _resolution(
    issue: str,
    evidence: list[Evidence],
    ids: dict[str, list[str]],
    expected: Decimal | None,
    captured: Decimal | None,
    refunded: Decimal,
) -> tuple[str, Decimal, list[dict[str, Any]], list[str]]:
    action_issues = {
        "canceled_order_paid",
        "unavailable_order_paid",
        "payment_mismatch",
        "duplicate_charge",
        "refund_pending",
        "refund_failed",
        "late_delivery_seller",
        "late_delivery_logistics",
    }
    entity = (ids["payment_references"] or ids["order_ids"] or [None])[0]
    amount, reason = Decimal(), None
    rule = _policy_rule(evidence, issue)
    policy_amount = _decimal(rule.get("refund_brl")) if rule else None
    if policy_amount is not None:
        amount = policy_amount
        reason = issue.upper() if amount > 0 else None
    elif (
        issue
        in {"canceled_order_paid", "unavailable_order_paid", "refund_failed", "refund_pending"}
        and captured
    ):
        amount, reason = max(Decimal(), captured - refunded), "OUTSTANDING_REFUND"
    elif issue == "duplicate_charge" and captured:
        amount, reason = max(Decimal(), captured - (expected or Decimal())), "DUPLICATE_CAPTURE"
    elif issue == "payment_mismatch" and captured and expected and captured > expected:
        amount, reason = captured - expected, "OVERCHARGE"
    lines = (
        []
        if not reason or amount <= 0
        else [{"reason_code": reason, "amount_brl": _money(amount), "entity_id": entity}]
    )
    actions = {
        "canceled_order_paid": ["ISSUE_OUTSTANDING_REFUND"],
        "unavailable_order_paid": ["ISSUE_OUTSTANDING_REFUND"],
        "payment_mismatch": ["RECONCILE_PAYMENT", "REFUND_CONFIRMED_OVERCHARGE"],
        "duplicate_charge": ["REFUND_DUPLICATE_CHARGE"],
        "refund_pending": ["MONITOR_OR_ESCALATE_REFUND"],
        "refund_failed": ["RETRY_OR_ESCALATE_REFUND"],
        "late_delivery_seller": ["REMEDIATE_SELLER_DELAY"],
        "late_delivery_logistics": ["REMEDIATE_LOGISTICS_DELAY"],
        "insufficient_evidence": ["REQUEST_ADDITIONAL_EVIDENCE"],
        "unsupported_claim": ["NO_ACTION"],
        "valid_split_payment": ["NO_ACTION"],
    }[issue]
    status = (
        "action_required"
        if issue in action_issues
        else ("needs_investigation" if issue == "insufficient_evidence" else "no_action")
    )
    if rule:
        policy_status = rule.get("case_status")
        policy_action = rule.get("recommended_action")
        if policy_status in {"action_required", "no_action", "needs_investigation"}:
            status = str(policy_status)
        if isinstance(policy_action, str) and policy_action:
            actions = [policy_action]
    return status, amount, lines, actions


def _root_cause(issue: str, evidence: list[Evidence], ids: dict[str, list[str]]) -> dict[str, Any]:
    party_type, party_id = "unknown", None
    if issue == "late_delivery_seller":
        party_type, party_id = "seller", (ids["seller_ids"] or [None])[0]
    elif issue == "late_delivery_logistics":
        party_type, party_id = "logistics_provider", (ids["shipment_ids"] or [None])[0]
    elif issue in {"payment_mismatch", "duplicate_charge", "refund_failed", "refund_pending"}:
        party_type, party_id = "payment_provider", (ids["payment_references"] or [None])[0]
    elif issue in {"canceled_order_paid", "unavailable_order_paid"}:
        party_type = "platform"
    elif issue in {"valid_split_payment", "unsupported_claim"}:
        party_type = "customer"
    rule = _policy_rule(evidence, issue)
    policy_parties = rule.get("responsible_parties") if rule else None
    if isinstance(policy_parties, list) and policy_parties:
        candidate = policy_parties[0]
        if isinstance(candidate, Mapping):
            party_type = str(candidate.get("party_type", party_type))
            party_id = candidate.get("party_id", party_id)
    if party_type == "seller" and party_id is None and ids["seller_ids"]:
        party_id = ids["seller_ids"][0]
    return {
        "ranked_causes": [{"cause_code": issue.upper(), "rank": 1}],
        "responsible_parties": [{"party_type": party_type, "party_id": party_id}],
    }


async def solve_case(
    case: dict[str, Any], gateway: EvidenceGateway, trace: TraceWriter
) -> dict[str, Any]:
    """Run the L3B specialists and verify the evidence-backed result."""
    from .l3b import solve

    return await solve(case, gateway, trace)
