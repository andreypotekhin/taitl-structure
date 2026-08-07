# Store Order Enrichment


`EnrichOrders` converts raw caller order rows into validated commercial order facts by applying tenant-scoped customer,
product, blocklist, promotion, and observed-shipment enrichment.


Orders missing required identity or product/customer references are not valid commercial demand. Identifiers are
normalized consistently, missing quantity and monetary values follow the documented defaults, and negative net totals do
not enter the accepted order relation. Shipment facts are observed inputs; enrichment does not create shipment actions.

The output preserves order-line identity and adds customer, product, promotion, and shipment-aware fields needed by
publication and downstream evaluation. Duplicate products on distinct lines remain distinct.

The accepted-demand predicate is conceptually:

```text
valid_line = required_identity
             and normalized_references
             and net_total >= 0
```

Promotion enrichment is evaluated at the order's business date, not by a current wall-clock lookup. This preserves
the distinction between historical demand facts and later fulfillment or shipment facts. An order records commercial
intent at its own grain; allocation and shipment are later observations that must not rewrite that historical fact.

## Design

The transform is streaming-compatible for an orders stream with caller-owned static lookups, but it does not own stream
lifecycle. A row-preserving customer lookup was chosen so missing customer detail remains visible while product and
block
eligibility remain explicit. Order enrichment and fulfillment planning are separate boundaries.


Tenant-safe lookups, required-field validation, negative-total handling, line identity, and online/generated parity are
stable. The caller remains responsible for writing the published order or taking a commercial action.


| Concern | Contract |
|---|---|
| Line identity | Order line identity remains unique even when product or customer enrichment multiplies source facts. |
| Required references | Tenant, order, product, quantity, and order-time fields are validated before enrichment. |
| Normalization | Monetary, status, address, and date fields use declared normalization policies. |
| Monetary facts | Negative or invalid totals follow an explicit validation/rejection policy. |
| Shipment | Shipment/planned delivery facts retain their own observed/planned identity. |
| Execution | Generated and online paths produce the same schema and values for the same snapshot. |

Enrichment is row-preserving with respect to the commercial line unless a separate one-to-many relation is named.
Missing customer detail remains visible and distinct from an invalid order. Product eligibility is referenced,
not silently applied as an order mutation; callers decide whether a commercial action is allowed.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Lookup | Alternatives in choices above | Row-preserving lookup | Keeps lineage explicit |
| Boundary | Alternatives in choices above | Enrich only | Keeps lineage explicit |
| Planned state | Alternatives in choices above | Separate planned facts | Keeps lineage explicit |

Failures must name tenant, order, line, field, and source snapshot. Evidence should cover missing customers,
duplicate products, negative totals, malformed required fields, and generated/online parity.


The corresponding implementation boundary is named by this document under `examples/store/transforms/`.
Its typed input/output definitions live under `examples/store/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
