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

## Design

The explicit filter/obtain/rerank stages were chosen over one opaque ranker so performance boundaries and evidence
movement remain explainable. Popularity-first retrieval was rejected because it can promote unrelated documents.
Feedback is applied after lexical admission so cold-start documents remain eligible without allowing feedback to invent
corpus candidates.

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

The pipeline is batch-oriented in this example. Candidate identity, score version, query key, and effective
snapshot travel through every stage. A candidate outside the admitted set cannot be introduced by feedback or
reranking; a missing optional artifact selects the lexical baseline.

The default feedback signal is `0.8 * query_document_feedback + 0.2 * document_popularity_feedback`. The final
rerank combines normalized lexical score and that feedback through caller-owned relevance-policy weights. This is a
policy formula, not a calibrated probability, and a missing component contributes zero while preserving the admitted
candidate.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Admission | Alternatives in choices above | Bounded funnel | Keeps lineage explicit |
| Caps | Global cap; per-stage cap; caller-owned | Per-stage cap | Each boundary has a measurable contract. |
| Feedback | Alternatives in choices above | Promote within admitted pool | Keeps lineage explicit |
| Streaming | Alternatives in choices above | Batch-only boundary | Keeps lineage explicit |

Failures must identify stage, query, candidate, cap, and snapshot. Evidence should cover empty history,
candidate 1,001, candidate 101 promotion, ties, and missing optional artifacts.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
