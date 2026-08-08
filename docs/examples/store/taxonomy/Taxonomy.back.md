# Store Product Taxonomy


Taxonomy expands product categories into bounded ancestor facts for category-aware retrieval, filtering, and
diversification.


Each product/category relationship may produce its category and valid ancestors with tenant identity and explicit depth
or lineage. Expansion is bounded and deterministic. Invalid parent links, cycles, or ambiguous tenant identity must not
silently create an unbounded hierarchy.

Taxonomy facts are descriptive structure. They do not by themselves make a product eligible, change inventory, or decide
which recommendation wins.

## How it works

Ancestor expansion is bounded rather than open-ended. Category identity is tenant-scoped rather than globally assumed.
A category graph service and unrestricted recursive state remain future options until their contracts are needed by more
than one Store boundary.


Products retain their original identity, ancestor order is stable, repeated paths are deduplicated deterministically,
and malformed taxonomy data produces an actionable failure or explicit rejection relation.


| Concern | Contract |
|---|---|
| Product | Taxonomy enrichment retains product identity; it does not create a new product. |
| Category | Category identity is tenant-qualified and stable across snapshots. |
| Ancestor | Ancestors carry depth and ordered path identity. |
| Expansion | Traversal is bounded by depth/row policy and rejects cycles. |
| Deduplication | Repeated paths resolve deterministically without losing provenance. |

Taxonomy expansion is a bounded relation transformation, not an unbounded graph walk. A product with multiple
paths can emit multiple declared memberships; downstream consumers choose whether to reduce them. A missing
parent, cycle, or conflicting category identity is a data-integrity issue, not an empty ancestry.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Traversal | Open recursion; flat lookup; bounded expansion | Bounded parent expansion | Work and depth stay bounded. |
| Identity | Global; product only; qualified path | Product/category/path keys | Tenant lineage survives. |
| Consumer | Collapse paths; hidden graph; preserve rows | Preserve hierarchy in rows | Consumers choose reduction. |

Failures should name product, category, parent, depth, and snapshot. Examples should include roots, multiple paths,
duplicate edges, missing parents, cycles, and the maximum-depth boundary.
