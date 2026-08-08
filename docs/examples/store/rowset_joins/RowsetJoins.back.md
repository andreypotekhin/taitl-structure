# Store Rowset Join Examples


The rowset-join boundary demonstrates explicit multi-row reconciliation and expansion shapes used around Store facts.
It is a teaching boundary, not a hidden replacement for catalog, order, or fulfillment workflows.


Examples may backfill customer relationships, expand customer/product combinations, reconcile orders and customers, or
demonstrate right, full, and Cartesian join semantics. The output schema states whether rows are preserved, filtered, or
intentionally multiplied.

Duplicate matches are meaningful where the example models a rowset relation. Keys and tenant identity remain explicit so
row multiplication cannot be mistaken for deduped lookup enrichment.

## How it works

Rowset joins remain separate from ordinary Store workflows to make cardinality visible. Lookup deduplication is not
implicit: a rowset example does not imply that production order or inventory paths may multiply rows without a business
contract.


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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Placement | Hidden join; workflow; dedicated examples | Dedicated examples | Cardinality stays visible. |
| Cardinality | Deduplicate; multiply; preserve shape | Preserve shape | Multiplication is explicit. |
| Reconcile | First row; implicit sum; policy reduction | Policy reduction | The rule stays visible. |

Failures must name join keys, tenant, expected cardinality, and source snapshots. Examples should include missing
matches, duplicate lookup keys, one-to-many expansion, many-to-many rejection, and an explicit reduction.
