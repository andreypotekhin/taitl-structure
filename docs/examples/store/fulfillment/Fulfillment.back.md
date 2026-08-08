# Store Fulfillment


`Fulfillment` plans how admitted commercial demand can be served from caller-provided inventory and inbound facts. It
keeps plans, shortages, substitutions, reconciliation, and summaries as separate typed evidence.


Demand admission applies the commercial checks needed before planning. Available-to-promise is on-hand minus reserved,
never below zero. Inbound inventory does not increase immediate allocation; it can supply a later planned ship date and
replenishment evidence.

The availability invariant is:

```text
available_to_promise = max(on_hand_quantity - reserved_quantity, 0)
```

This is a planning fact, not a reservation mutation. Inbound quantity can explain a later planned date or a
replenishment suggestion, but it cannot make a line immediately allocated. Plans describe intended action, while
observed shipment facts describe what happened; keeping those states separate makes service evaluation meaningful.

For each demand line, the planner selects one active tenant-scoped warehouse by region preference, warehouse priority,
available-to-promise, and warehouse ID. The first contract does not split a line across warehouses. Status is allocated,
partially allocated, or backordered. A replenishment suggestion is descriptive and never a purchase order.

Reconciliation compares planned lines with observed shipment facts by tenant, order, and line. Service evaluation keeps
unknown shipment dates unknown and distinguishes planned warehouse from actual warehouse when the latter is absent.

## How it works

Allocation starts with one explicit warehouse for deterministic first-slice behavior. Splitting, dynamic promise
calendars, reservation consumption, procurement, and actual warehouse attribution remain future boundaries. Fulfillment
stays separate from shipping execution and order publication.


No line is allocated from negative availability, duplicate product lines remain separate, planned and observed facts are
not conflated, and every suggestion or exception exposes its reason and policy inputs.


| Concern | Contract |
|---|---|
| Demand | Order lines remain distinct and carry tenant, product, quantity, and requested timing. |
| Availability | Available-to-promise is nonnegative and snapshot-aligned. |
| Warehouse | Allocation is warehouse-qualified; the current baseline uses one declared warehouse policy. |
| Plan | Allocation, replenishment, and inbound suggestions are planned facts with policy identity. |
| Observed | Shipment/receipt facts are observed evidence and are never used as silent plan overwrites. |
| Reconciliation | Exceptions retain source facts, difference, and policy/reason reason. |

Allocation is a proposal relation, not a reservation or shipment action. It must be deterministic for a fixed
input snapshot and must not allocate more than available quantity. Split shipment, dynamic promise calendars,
and procurement are separate extensions because each changes cardinality or ownership of state.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Warehouse | Any; one warehouse; split allocation | One explicit warehouse | First slice stays deterministic. |
| Split | Silent split; one line; deferred relation | Deferred relation | Future allocation needs its own grain. |
| Inbound | Mutate supply; ignore; separate suggestion | Separate suggestion | Procurement is caller-owned. |
| Replenishment | Mutate inventory; emit proposal; external planner | Emit proposal | Callers own procurement actions. |

Failures must identify tenant, line, warehouse, availability snapshot, and policy. Examples should cover zero and
negative availability, duplicate lines, partial availability, inbound supply, and planned/observed divergence.
