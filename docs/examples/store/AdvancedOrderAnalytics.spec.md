# Store Advanced Order Analytics


`AdvancedOrderAnalytics` is a teaching-oriented boundary for grouped rollups, cubes, customer windows, and collection
profiles over fulfilled order facts.


Outputs retain the grouping dimensions and distinguish subtotal, cube, window, and collection-profile meanings. A
rollup or cube row is not an order row and must not be fed into fulfillment as if it were demand.

## Design

Advanced analytics is kept separate from the primary `OrderAnalytics` path so its richer shapes do not obscure ordinary
reporting. It demonstrates Structure capabilities without claiming that every analytical shape is a production Store
contract.


Group dimensions and null/subtotal semantics are explicit, tenant identity is retained, and analytical outputs remain
downstream descriptive facts.


| Output | Grain | Semantics |
|---|---|---|
| Rollup | Declared dimension group | Additive measures with explicit null and empty-group policy. |
| Cube | Dimension combination plus subtotal marker | Subtotals are identified, not inferred from null alone. |
| Window | Entity and ordered event/date key | Frame, ordering, and tie policy are contractual. |
| Profile | Customer/product/order group | Descriptive features retain tenant and source snapshot. |

Null dimensions, empty groups, and subtotal rows have separate meanings. Grouped output retains the group
definition, business date, tenant, and source snapshot. Window results use a stable order key and do not assume
that the input relation is already sorted.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Separation | Alternatives in choices above | Dedicated boundary | Keeps lineage explicit |
| Grouping | Alternatives in choices above | Explicit marker | Keeps lineage explicit |
| Production use | Alternatives in choices above | Descriptive snapshot | Keeps lineage explicit |

Failures should name dimensions, frame/order policy, tenant, date, and source snapshot. Evidence must include
empty groups, null business values, subtotals, ties in a window, and repeated computation.


The corresponding analytics boundaries are named by this document under `examples/store/transforms/analytics/`.
Their typed input/output definitions live under `examples/store/schemas/adv_analytics.py`. The transforms describe
composition and lifecycle; the schema defines dimensions, measures, nullability, and grouping markers. Those source
paths orient an implementation reader, but the contract above is intentionally consumable without opening them.
