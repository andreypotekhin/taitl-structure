# Search Sentence Presentation


`SearchSentences` presents the narrowest lexical evidence grain for callers that need exact sentence-level matches.


It accepts one or more queries, the original documents, immutable sentence boundaries, and sentence score relations. It emits one-based ranks partitioned
by query and experiment. Ordering is descending BM25, descending overlap, then stable document and sentence IDs.

The result is a relation, not an ordered collection. Consumers must page or sort using the emitted rank and must not
depend on physical Spark row order.

## Design

Sentence search remains separate from passage and document search because the result meaning and rank grain differ.
Returning only one best sentence was rejected because the boundary is an evidence presenter; top-K and deduplication
belong to the caller.


Multiple queries remain isolated, ranks restart per query and experiment, ties are deterministic, and score freshness
comes from the shared scoring contract.


| Concern | Contract |
|---|---|
| Input | Sentence facts retain document, paragraph, query, experiment, and score snapshot identity. |
| Rank partition | Rank restarts per query and experiment, never across the full relation. |
| Ordering | Primary score order is followed by deterministic sentence/document identity. |
| Result | Published rows expose score, rank, parentage, and policy/version identity. |
| Physical order | DataFrame order is implementation detail and has no semantic effect. |

Query identity is a key, not merely a filter applied after ranking. Every join and aggregation must preserve
that key, so one query cannot borrow a sentence, score, or rank from another query. Empty query results are valid
outputs; malformed rank partitions are not.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Result unit | Alternatives in choices above | Sentence with parentage | Keeps lineage explicit |
| Ordering | Alternatives in choices above | Score plus stable identity | Keeps lineage explicit |
| Query scope | Alternatives in choices above | Query plus experiment rank | Keeps lineage explicit |

Failure evidence must include query, experiment, candidate, rank partition, and score version. Fixtures should
interleave queries, include ties, and reorder input rows without changing output.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
