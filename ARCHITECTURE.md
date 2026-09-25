# L3B Architecture Record

## Coordination and responsibilities

`workflow.solve_case` delegates L3B coordination and analysis to `l3b.py`.
The coordinator assigns work to entity/customer, order/product, shipment,
payment/refund, policy, and verifier roles. These are deterministic specialist
stages, not separate language-model processes. Handoffs are recorded in the public
trace; no private reasoning or customer text is logged.

## Evidence collection

The gateway discovers tool names and input schemas once per MCP session. Each
case has its own connection; up to four cases execute concurrently, while calls
within a case remain ordered. Every
request is checked against its advertised schema and includes the current case ID.
The workflow uses only advertised tools. Each returned envelope is validated
against the public evidence contract before its original reference is consumed.
A per-case argument cache prevents duplicate requests, including repeated failures.
No evidence is cached across cases or runs. One retry is allowed for connection or
timeout errors; tool and validation errors are not retried.

A normal case uses seven calls: customer history, order, items, product context,
shipment summary, payment timeline, and policy. Refund timeline is added only for
claims that can be about a pending/failed refund or a paid canceled/unavailable
order. Payment timeline already includes payment records, so a redundant
payment-summary call is avoided. Seller identifiers come from item evidence. Tool
failures lower confidence; missing refund evidence produces null refund totals, not
fabricated zero totals.

## Entity resolution and source conflicts

Candidate identifiers are checked against customer history and order evidence.
A claimed identifier is not sufficient on its own. Where an identifier has several
snapshots, only purchases on or before case opening are eligible, and the latest
eligible snapshot is selected. Competing eligible identifiers or inconsistent
snapshots at the same purchase timestamp remain ambiguous. Candidates absent from
returned customer history and eligible order records are rejected within that
customer scope. The workflow does not query arbitrary unrelated orders.

Dated payment, refund, and shipment events are limited to the selected purchase
interval, ending at the next purchase snapshot for the same identifier. Item
versions are matched by shipping deadline proximity to the selected purchase.
This temporal matching is a documented heuristic when the source omits explicit
version identifiers. Differences between direct order/shipment summaries and the
selected customer-history snapshot are recorded with tool source names and a
resolution code. Raw evidence and evidence references are never rewritten; only
the in-memory analytical view selects relevant rows.

## Decisions and accounting

The workflow independently reports entity resolution, customer history, shipment
and payment findings, primary and secondary issues, claim assessments, source
conflicts, and policy-based resolution. It computes amounts with Decimal, separates
split payments from excess captures, and does not call a single capture a mismatch
when the source has no explicit reconciliation signal. It distinguishes absent
refund evidence from a successful empty refund history. Policy amounts are capped at the observed
remaining captured balance when available. Seller responsibility must use an ID
from the resolved order evidence. Missing core evidence or unresolved entities
produce an investigation result with no recommended refund.

## Independent verification and trace

The verifier checks the public L3B output schema, current case ID, consumed-reference
membership, resolved/rejected order disjointness, affected-order consistency,
refund line sums, and zero refunds for no-action cases. It emits
verification_completed only after those checks succeed. The CLI validates again
before atomically writing each output. Case traces are buffered until verification
and connection cleanup succeed. A transient broken connection permits one case
restart with fresh evidence; a second failure stops that task. Interrupted attempts
can still count toward the server audit even when they do not produce an output. Trace events are case_received,
task_assigned, tool_result_consumed, handoff, policy_decided,
verification_completed, and case_finalized. The verification event reports a count
of failed tool calls without exposing response bodies or credentials.

## Reproducibility and packaging

From the repository root, activate the environment and run:

```powershell
day09 validate-inputs
day09 run
day09 validate
day09 package --output dist/submission.zip
```

A default run creates fresh outputs and trace using live, audited MCP references.
Use `day09 run --resume --workers 4` to continue the same interrupted competition
run with the same team, endpoint, and case inputs. Resume validates existing outputs
and their trace provenance before skipping completed cases. Partial traces are
backed up under traces/interrupted-*.jsonl and excluded from the submission. Do not
reuse resume after resetting the server run or changing teams/input versions. Packaging
includes only manifest.json, trace.jsonl, and the 100 outputs. Source, input files,
.env, credentials, and debug files are excluded. Local validation establishes
schema and accounting correctness; competition semantic/provenance scoring remains
server-side. Regression tests use synthetic evidence and make no MCP calls.
