# Store Rowset Join Examples


The rowset-join boundary demonstrates explicit multi-row reconciliation and expansion shapes used around Store facts.
It is a teaching boundary, not a hidden replacement for catalog, order, or fulfillment workflows.


Examples may backfill customer relationships, expand customer/product combinations, reconcile orders and customers, or
demonstrate right, full, and Cartesian join semantics. The output schema states whether rows are preserved, filtered, or
intentionally multiplied.

Duplicate matches are meaningful where the example models a rowset relation. Keys and tenant identity remain explicit so
row multiplication cannot be mistaken for deduped lookup enrichment.

## Design

Rowset joins are kept separate from ordinary Store workflows to make cardinality visible. Silent lookup deduplication
was rejected. A rowset example does not imply that production order or inventory paths may multiply rows without a
business contract.


Join cardinality is documented, output keys remain inspectable, tenant conditions are explicit, and the examples do not
change the semantics of the main Store funnels.


| Shape | Cardinality | Required evidence |
|---|---|---|
| Enrichment | One-to-one or many-to-one | Lookup key, uniqueness rule, and missing-match policy. |
| Expansion | One-to-many | Parent key, child key, and declared row multiplication. |
| Reconciliation | Many-to-one reduction | Reduction key, measure rule, and duplicate policy. |
| Tenant join | Same tenant scope | Tenant condition is part of the join, not a post-filter. |
| Output | Named business grain | All keys needed to recover the source relationship remain visible. |

The examples distinguish a relationally valid join from a business-valid join. A many-to-many result is not
silently reduced to one row, and a missing match is not silently converted into a zero measure. Consumers must
choose whether to preserve the expanded grain or aggregate it with an explicit reconciliation rule.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Placement | Alternatives in choices above | Dedicated examples | Keeps lineage explicit |
| Cardinality | Alternatives in choices above | Declare and preserve shape | Keeps lineage explicit |
| Reconcile | Alternatives in choices above | Policy-keyed reduction | Keeps lineage explicit |

Failures must name join keys, tenant, expected cardinality, and source snapshots. Evidence should include missing
matches, duplicate lookup keys, one-to-many expansion, many-to-many rejection, and an explicit reduction.


The corresponding implementation boundary is named by this document under `examples/store/transforms/`.
Its typed input/output definitions live under `examples/store/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
