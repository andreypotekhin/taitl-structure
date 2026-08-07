# Search Similarity Presentation


`SearchSimilarity` presents same-grain corpus-neighbor relations as caller-friendly lookup results. It does not create
an embedding service or impose product filters on the corpus.


The boundary accepts one source target, the corresponding corpus targets, and same-grain similarity pairs. It emits up
to the configured result limit, preserving source identity and corpus metadata. Ranking follows the query-to-candidate
directed BM25 direction, then overlap and stable target identifiers.

Document, section, paragraph, and sentence presentation remain grain-isolated. Title, source, language, and collection
filters are caller decisions after similarity scoring.

## Design

Lexical similarity was chosen as the current portable baseline. A single symmetric score was rejected because BM25 is
directional and corpus-dependent. Vector similarity is an opt-in future lane that would fuse ranks with RRF rather than
blend raw BM25 and cosine values.


Same-grain relations are respected, self-pairs are absent, output limits are deterministic, and callers can inspect the
evidence that caused a neighbor to rank.


| Concern | Contract |
|---|---|
| Source | A source target and its grain are explicit. |
| Candidate | A neighbor has the same declared grain and compatible snapshot identity. |
| Metadata | Document/section/paragraph/sentence parentage remains inspectable. |
| Ordering | Similarity score is followed by canonical candidate identity. |
| Limit | The limit applies per source and is deterministic under ties. |
| Filters | Eligibility filters run before publication and do not erase evidence fields. |

Similarity is directed at the evidence level: source A may retain a reason that is not identical to source B's
reason. Self-pairs are excluded by identity, not by assuming the first row is the source. The published relation
may be reduced for display, while evidence remains available for evaluation.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Evidence | Alternatives in choices above | Inspectable relation | Keeps lineage explicit |
| Filtering | After top-k; before top-k; caller-only | Before top-k | Ineligible candidates cannot consume the limit. |
| Future semantics | Alternatives in choices above | Directed evidence boundary | Keeps lineage explicit |

Failures must identify source, candidate, grain, limit, and filter snapshot. Evidence should cover self-pairs,
cross-grain candidates, ties, sparse candidates, and a limit of zero.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
