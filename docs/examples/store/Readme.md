# Store Example Backgrounds

These specifications describe the Store example by business boundary. Store is a caller-owned typed transformation
example for tenant-scoped catalog, recommendations, demand, fulfillment, observed shipment facts, and analytics.
Planned decisions, commercial demand, observed outcomes, and descriptive summaries stay separate so a plan is not
mistaken for a guarantee, a recommendation is not mistaken for demand, and a rollup is not mistaken for a base row.

The layout follows the public top-level packages under `examples/store/transforms/`. Each background accumulates its
contract, formulas, alternatives, resolved choices, and failure evidence so the design can be read offline without
reconstructing decisions from separate documents or discussions.

## Boundaries

- Catalog — tenant-scoped product eligibility and normalization.
- Taxonomy — bounded category ancestry.
- Recommender — candidate generation, ranking, diversification, and signals.
- Personalization — explicit customer and session preferences.
- Merchandising — recommendation-serving composition.
- Orders — commercial order enrichment and publication.
- Fulfillment — demand admission, allocation, planning, and reconciliation.
- Evaluation — recommendation behavior and fulfillment service evidence.
- Experiments — variant assignment, exposure, and descriptive comparison.
- Analytics — order and fulfillment summaries.
- Rowset joins — explicit row-multiplying and reconciliation examples.
- Advanced analytics — teaching-oriented rollups, cubes, and windows.

## Shared Store rules

Store is a typed transformation example, not an e-commerce service. Callers own source systems, persistence, Spark
and stream lifecycle, reservations, procurement, payments, shipping actions, customer communications, and deployment.
That boundary is deliberate: transformations can explain eligibility, demand, allocation, and measurement without
claiming authority to mutate the systems that make those decisions real.

Tenant identity is part of business identity throughout catalog, orders, recommendations, inventory, fulfillment, and
evaluation. Planned facts, commercial demand, and observed shipment facts remain separate. Every ranking, allocation,
fallback, and summary tie-breaker must be deterministic and explainable.

## Architecture map

| Plane | Boundaries | Primary artifact | State owner |
|---|---|---|---|
| Merchandising | Catalog/Taxonomy/Recommender/Personalization | Eligible/ranked products | Caller-owned request state |
| Commerce | Orders, Rowset joins | Enriched commercial demand | Caller-owned order source |
| Operations | Fulfillment | Allocation, plan, shipment facts | Caller-owned inventory sources |
| Measurement | Evaluation/Experiments/Analytics/Advanced | Evidence and summaries | Caller-owned event/run identity |

## Shared architectural decisions

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Service boundary | Stateful/hidden effects/typed transforms | Typed transformations | Composable and testable. |
| Identity | Global keys or tenant-qualified keys | Tenant-qualified keys | Keys cannot cross a business partition. |
| Time semantics | One status/event/planned-observed | Planned/observed split | Plans can be checked against outcomes. |
| Fallback | Silent nulls/random/explicit baseline | Explicit baseline | Sparse history stays deterministic. |

The focused specifications use the same vocabulary: grain, identity, snapshot, planned-versus-observed state, and
caller-owned side effects. The implementation boundaries named by this index live under `examples/store/transforms/`;
their field, identity, and nullability definitions live under `examples/store/schemas/`. The paths provide source
orientation but are not required reading: the prose defines the joins, cardinality, freshness, and failure evidence
without turning the documents into annotated source walkthroughs.
