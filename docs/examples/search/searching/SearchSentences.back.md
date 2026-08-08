# Search Sentence Presentation


`SearchSentences` presents the narrowest lexical evidence grain for callers that need exact sentence-level matches.


It accepts one or more queries, the original documents, immutable sentence boundaries, and sentence score relations. It
emits one-based ranks partitioned
by query and experiment. Ordering is descending BM25, descending overlap, then stable document and sentence IDs.

The result is a relation, not an ordered collection. Consumers must page or sort using the emitted rank and must not
depend on physical Spark row order.

## How it works

Sentence search remains separate from passage and document search because the result meaning and rank grain differ. The
boundary presents sentence evidence with parentage; top-K and deduplication remain caller choices rather than being
forced into one best sentence.


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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Result unit | Whole document; best sentence; sentence + parent | Sentence + parent | Callers retain lineage. |
| Ordering | Physical order; score only; score + ID | Score + ID | Ties stay stable. |
| Query scope | Global rank; query rank; query + experiment | Query + experiment | Ranks stay query-local. |


Failures should identify query, experiment, candidate, rank partition, and score version. Examples should interleave
queries, include ties, and reorder input rows without changing output.
