# Search Similarity Presentation


`SearchSimilarity` presents same-grain corpus-neighbor relations as caller-friendly lookup results. It adopts lexical
and vector candidate lanes, fuses them with Reciprocal Rank Fusion (RRF), and reranks the fused candidates before
joining corpus metadata. It does not create an embedding service or impose product filters on the corpus.


The boundary accepts one source target, the corresponding corpus targets, lexical similarity pairs, ranked vector
candidates, and the vector retrieval policy. It emits up to the configured result limit, preserving source identity and
corpus metadata. Ranking follows RRF, then vector similarity, lexical evidence, and stable target identifiers.

Document, section, paragraph, and sentence presentation remain grain-isolated. Title, source, language, and collection
filters are caller decisions after similarity scoring.

## How it works

Lexical similarity remains the portable baseline. BM25 is directional and corpus-dependent, so the relation preserves
inspectable source-to-candidate evidence rather than forcing one symmetric score. Vector candidates are an explicit
lane; lexical-only callers supply an empty vector-candidate relation and a single RRF policy row.


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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Evidence | Opaque list; scalar score; relation | Inspectable relation | Evidence stays inspectable. |
| Filtering | After top-k; before top-k; caller-only | Before top-k | Ineligible candidates cannot consume the limit. |
| Future semantics | Raw vector/BM25 blend; replace; directed boundary | Directed boundary | Direction stays explicit. |


Failures must identify source, candidate, grain, limit, and filter snapshot. Useful examples cover self-pairs,
cross-grain candidates, ties, sparse candidates, and a limit of zero.
