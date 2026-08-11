# Search Document Presentation


`SearchDocuments` is the request-time document presentation boundary. It combines cached and online lexical artifacts,
bounded candidate admission, reusable user context, and feedback reranking into a deterministic result list.

It is the point where a prepared Search snapshot meets one request. The boundary does not build the corpus, learn a
ranker, or write serving state; it selects and explains a bounded result set from caller-owned artifacts. This makes
the result list useful both as a user-facing output and as evidence for later evaluation.


The funnel retains at most 10,000 filter targets, admits at most 1,000 composite lexical candidates, and returns at
most 100 final results after feedback reranking. Feedback may reorder admitted candidates but cannot create a new one.
Missing feedback contributes zero. Results expose candidate rank, final rank, score, feedback, and final rank score.

Results are partitioned by query, user context, and experiment. Query/request timestamps must match. Score and filter
artifacts must be no later than the request, within maximum age, and at or after the policy effective timestamp.

In practical terms, a query with no feedback still follows the lexical path, while a query with feedback can promote
an already-admitted document. A stale or incompatible artifact is not silently mixed into the result: the baseline
remains available and the identity mismatch is diagnosable. The request, candidate, policy, and snapshot keys are
therefore as important as the final score.

## How it works

The explicit filter, obtain, and rerank stages keep performance boundaries and evidence movement explainable. Lexical
admission comes before feedback, so cold-start documents remain eligible while feedback cannot invent corpus candidates
or promote an unrelated document through popularity alone.

SearchDocuments currently declares streaming lineage but is `batch_only`; global ranking, deduplication, and joins need
a
bounded state design before a streaming claim.


Candidates outside the admitted set cannot appear, candidates ranked 101–1,000 can be promoted into the top 100,
no-history queries preserve lexical order, and ties are stable by document ID.


| Stage | Target bound | Contract |
|---|---:|---|
| Filter | 10,000 | Admit only candidates satisfying the declared eligibility policy. |
| Obtain | 1,000 | Materialize bounded evidence and preserve candidate identity. |
| Feedback | As available | Lookup snapshot-aligned behavior without removing lexical candidates. |
| Rerank | 1,000 | Apply optional scores within query and experiment scope. |
| Publish | 100 | Emit deterministic top results with stable ties and lineage. |

The source composition is intentionally legible at this boundary:

```python
selected = SelectFilterTargets(...).targets
candidates = RetrieveDocuments(prefilter_targets=selected).candidates
results = RerankDocuments(candidates=candidates, ...).results
```

The sketch shows the semantic handoff, not a complete caller invocation. The full transform also carries query,
request, policy, score, user-band, and feedback relations through those stages.

## Field-search delegation

Canonical `SearchDocuments` is field-unaware and keeps the ordinary cache-aware filtering contract. `SearchFields`
delegates body text by creating a child `SearchQuery` and child request whose content contains only the body portion of
the field-aware query. The child ID is distinct from the parent because its content and cache identity differ; published
`FieldSearchResult` rows expose the parent ID and may contain the nested document result with its parent ID remapped.

When metadata clauses are present, `SearchFields` also sends
`document_filter_targets(query_id=delegated_child_id, document_id=...)` to a companion document-search funnel under
`searching/search_fields`. The companion inherits the canonical stages and replaces filtering and target selection so
that target-bearing queries are restricted before filter ranking and the 10,000-document cap. Ordinary direct
`SearchDocuments` queries continue through the canonical path with no target relation.

This separation keeps field syntax, metadata phrase matching, and parent/child publication at `SearchFields`, while
keeping document scoring, retrieval, and feedback reranking reusable and field-unaware. Full-corpus score
normalization and score-cache semantics are unchanged by delegation.

The pipeline is batch-oriented in this example. Candidate identity, score version, query key, and effective
snapshot travel through every stage. A candidate outside the admitted set cannot be introduced by feedback or
reranking; a missing optional artifact selects the lexical baseline.

The default feedback signal is `0.8 * query_document_feedback + 0.2 * document_popularity_feedback`. The final
rerank combines normalized lexical score and that feedback through caller-owned relevance-policy weights. This is a
policy formula, not a calibrated probability, and a missing component contributes zero while preserving the admitted
candidate.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Admission | Popularity first; lexical only; bounded funnel | Bounded funnel | Feedback cannot invent rows. |
| Caps | Global cap; per-stage cap; caller-owned | Per-stage cap | Each boundary has a measurable contract. |
| Feedback | Create; rerank; promote admitted | Promote within admitted pool | Cold-start rows remain eligible. |
| Streaming | Claim streaming; reject requests; batch until proof | Batch-only boundary | Claim only proven state. |
| Field delegation | Teach `SearchDocuments` field syntax; intersect after retrieval; target-aware companion | Target-aware companion | Metadata targets must constrain filter ranking before its cap while canonical document search remains reusable. |


Failures must identify stage, query, candidate, cap, and snapshot. Useful examples cover empty history,
candidate 1,001, candidate 101 promotion, ties, and missing optional artifacts.
