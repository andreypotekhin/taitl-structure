# Store Analytics


Analytics summarizes orders, fulfillment plans, allocation load, and service outcomes after the operational boundaries
have produced typed facts.


Order analytics publishes tenant- and date-scoped customer and product summaries and deterministic event ranks.
Fulfillment analytics publishes daily service-risk and warehouse-load summaries from plans and allocations. Advanced
analytics extends these facts with explicit rollups, cubes, and customer windows.

Analytics is descriptive. It does not forecast demand, alter a plan, allocate inventory, or publish a financial ledger.

Every summary begins by fixing its grain—for example, `(tenant_id, customer_id, business_date)` for a customer-day
total or `(tenant_id, product_id, business_date)` for a product-day summary. Rollup and cube nulls are structural
subtotals and must be interpreted with grouping metadata, not as missing source values. The separation is essential:
descriptive aggregation may explain operational facts, but must never mutate the demand, allocation, or shipment facts
from which it was produced.

## How it works

Analytics remains downstream of decision and observation facts so reports cannot silently become inputs to fulfillment.
Batch summaries were chosen for the current example; real-time dashboards, approximate sketches, and accounting-grade
financial measures require their own freshness and correctness contracts.


Summaries preserve tenant and business-date identity, empty groups are represented according to the declared schema,
and adding analytics does not change upstream commercial or fulfillment outputs.


| Output | Key | Freshness/use |
|---|---|---|
| Order summary | Tenant/order/business date | Snapshot-aligned descriptive fact. |
| Fulfillment summary | Tenant/warehouse/business date | Planned and observed measures remain separate. |
| Service metric | Tenant/metric/window | Denominator and attribution policy are retained. |
| Group summary | Declared dimensions plus subtotal marker | Empty and null groups follow schema policy. |

Analytics reads stable source relations and emits new facts; it does not update inventory, order state, or
customer state. Measures name whether they are counts, amounts, rates, or durations and expose the population
used for a rate. Business date is distinct from ingestion timestamp.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Downstream role | Operational input; ledger; summaries | Descriptive summaries | Reports do not drive fulfillment. |
| Refresh | Wall-clock query; bounded batch; opaque cache | Bounded batch | Comparisons and replay are reproducible. |
| Financial meaning | Ledger authority; raw; amount | Descriptive amount | Accounting authority stays external. |

Failures should name measure, population, date/window, tenant, and source snapshot. Examples should cover empty
groups, duplicate lines, planned versus observed fulfillment, and a rate with zero denominator.
