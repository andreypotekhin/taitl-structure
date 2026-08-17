# Search Online Serving

Online serving resolves request-time gaps against caller-owned Search artifacts. It validates cached filter and score
rows, selects admitted document targets, fills query and selected-document vector gaps, and merges fresh evidence into
one authoritative request scope.

The order protects cost and correctness. `OnlineFiltering` resolves missing filter groups and caps document targets;
`OnlineVectorization` infers only the request query and admitted document gaps; `OnlineScoring` calculates missing or
stale lexical/vector score groups and merges them before retrieval and ranking.

## How it works

- Match request query IDs to fresh filter artifacts under the active policy.
- Compute filter gaps and merge usable stored/online rows into a bounded `DocumentSearchTarget` scope.
- Select vector gaps from the request query and admitted document targets only.
- Merge compatible cached and inferred embeddings, retaining inference statuses.
- Resolve lexical/vector score gaps within the target scope and invalidate complete vector score groups when needed.
- Expose one authoritative score relation to retrieval, fusion, and feedback.

Freshness is policy-relative: an artifact must be effective for the request, no newer than the request timestamp, and
within the configured maximum age. Query identity, target scope, score policy, vector policy, model identity, and source
snapshot belong to the cache contract.

| Boundary | Contract |
|---|---|
| Filter gap | Missing or stale groups are recalculated from reusable indexes. |
| Target scope | Cached and online filter rows are merged and capped deterministically. |
| Vector gap | Only the request query and admitted documents are sent to inference. |
| Score gap | Missing/stale lexical and vector groups are recalculated within scope. |
| Retrieval | Consumes merged evidence after freshness and identity validation. |

Failures should identify request, query, target scope, artifact timestamp, policy, and model identity. Examples should
cover cache hits, stale and future rows, filter gaps, vector gaps, provider failures, vector-only candidates, and
lexical fallback.
