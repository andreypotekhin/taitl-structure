# Store Merchandising


`Merchandising` is the app-facing recommendation-serving composition. It connects product preparation, candidate
generation, ranking, feedback, and publication without becoming a serving service.


The boundary consumes tenant-scoped products, blocked products, promotions, policy, optional boosts and suppressions,
and caller-provided feedback facts. It emits ranked recommended products and recommendation-run metadata suitable for a
caller to serve and log.

Recommendation evaluation is not a required input to serving. A served run must be recorded by the caller before its
impressions can become future evidence.

## Design

The composition keeps evaluation outside serving to avoid circular dependencies and to permit zero-result requests to be
measured independently. It also keeps actual inventory and fulfillment decisions outside merchandising; inventory boost
is evidence, not a reservation.


The composition remains usable with empty optional signals, preserves tenant and request identity, exposes the selected
strategy/policy version, and produces stable output schemas for caller-owned serving.


| Concern | Contract |
|---|---|
| Inputs | Catalog eligibility, taxonomy, recommendations, personalization, and policy snapshots are explicit. |
| Output | Product, rank, score, strategy, request, tenant, and policy identity are retained. |
| Evidence | Optional signals remain distinguishable from zero-valued signals. |
| Lifecycle | Serving output is a snapshot; reservation, checkout, and shipment are external actions. |
| Optional branches | Missing personalization or experiments preserve a documented baseline. |

Merchandising composes candidate and policy facts; it does not become an inventory authority. A selected strategy
is metadata for interpretation, not a hidden side effect. Evaluation consumes serving evidence downstream and must
not feed back into the same composition without an explicit snapshot boundary.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Serving | Alternatives in choices above | Recommendation relation | Keeps lineage explicit |
| Evaluation | Alternatives in choices above | Downstream evidence | Keeps lineage explicit |
| Inventory | Alternatives in choices above | Boost/evidence only | Keeps lineage explicit |

Failures must identify request, tenant, strategy, policy, and missing branch. Evidence should cover empty optional
signals, no eligible products, strategy ties, stale snapshots, and baseline equivalence.


The corresponding implementation boundary is named by this document under `examples/store/transforms/`.
Its typed input/output definitions live under `examples/store/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
