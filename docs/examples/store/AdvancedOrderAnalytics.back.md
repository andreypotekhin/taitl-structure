# Store Advanced Order Analytics


`AdvancedOrderAnalytics` is a teaching-oriented boundary for grouped rollups, cubes, customer windows, and collection
profiles over fulfilled order facts.


Outputs retain the grouping dimensions and distinguish subtotal, cube, window, and collection-profile meanings. A
rollup or cube row is not an order row and must not be fed into fulfillment as if it were demand.

## How it works

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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Separation | Primary; mixed; dedicated boundary | Dedicated boundary | Richer shapes stay separate. |
| Grouping | Null inference; grouping marker; subtotal row | Grouping marker | Subtotals are marked. |
| Production use | Serving; write-back; descriptive snapshot | Descriptive snapshot | Analytics stay descriptive. |

Failures should name dimensions, frame/order policy, tenant, date, and source snapshot. Examples should include
empty groups, null business values, subtotals, ties in a window, and repeated computation.
