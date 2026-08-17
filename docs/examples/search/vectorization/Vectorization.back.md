# Search Vectorization

`Vectorization` invokes the shared inference boundary for the query and document relations supplied by a Search facet.
It is provider-neutral: it does not decide which rows are missing from a cache or where embeddings are persisted.

Offline and online facets add the ownership needed around this boundary. `OfflineVectorization` supplies the complete
document population and merges inferred embeddings into a caller-owned snapshot. `OnlineVectorization` selects query
and admitted-document gaps, invokes inference for those gaps, and merges successful results with usable cache rows.
Provider embeddings are normalized into the common vector-query relation before scoring.

## How it works

- Run `Inference` for the supplied query and document relations.
- Normalize request embeddings into `DocumentVectorQuery` rows keyed by query identity.
- Preserve similarity source identity in `VectorizeSimilarityQueries` so self-exclusion remains explicit.
- Let offline preparation infer the complete snapshot and let online serving infer only request-time gaps.
- Expose embeddings and inference statuses for caller-owned persistence, diagnostics, and cache warming.

The online order matters: filtering first bounds admitted documents, vectorization fills only those document gaps, and
scoring consumes the merged vector relations. A failed vector lane does not make lexical results unavailable.

| Boundary | Contract |
|---|---|
| Shared vectorization | Calls inference for caller-supplied query/document relations. |
| Offline facet | Uses the complete document population and non-streaming provider mode. |
| Online facet | Uses streaming provider mode and selected query/document gaps. |
| Query normalization | Binds embeddings to SearchQuery identity and emits common vector queries. |
| Cache merge | Keeps compatible rows and replaces only missing or invalidated groups. |

Diagnostics should distinguish cache hits, gaps, invalid compatibility identity, provider failures, and filtered-out
documents. Examples should cover complete offline snapshots, online query gaps, selected-document gaps, cache reuse,
failed inference, and generated/online parity.
