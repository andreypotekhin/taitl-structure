# Search Document Filtering


Filtering provides a cheap, reusable candidate boundary before composite lexical scoring and feedback reranking.


The filter counts distinct normalized query terms shared with each document. It orders by descending matched-term count,
then document ID, and publishes timestamped `DocumentFilterScore` rows. `Filtering` creates offline artifacts for a
selected query set; `OnlineFiltering` computes only query groups missing or invalid in the cache.

`SelectFilterTargets` applies request-time validity rules and retains at most 10,000 document targets per query. A
filter
artifact cannot be used when it is future-dated, older than the configured maximum age, or older than the policy's
effective timestamp.

## Design

Filtering was moved before retrieval to create a clear performance boundary. The old post-retrieval overlap narrowing
shape was rejected because it could not bound the expensive candidate set. IDF-weighted overlap remains a scoring
feature; the filter deliberately uses simple distinct-term counts.


Offline and online artifacts share one schema, cache gaps are resolved without recomputing usable groups, ties are
deterministic, and target count never exceeds the declared cap.

The admission score is intentionally cheaper than lexical relevance:

```text
matched_terms(q, d) = |distinct_normalized_query_terms ∩ indexed_document_terms(d)|
```

Rows order by matched-term count descending and document ID ascending before the 10,000-target cap. The
This is a cost boundary: cheap deterministic admission happens first, while IDF-weighted overlap is reserved for
composite scoring after admission. Separating the stages keeps candidate volume bounded without discarding the richer
evidence needed for ranking.


Invalid policy timestamps, duplicate conflicting filter rows, future-dated artifacts, and inconsistent query identity
must be diagnosed. Evidence should include ties, zero-term queries, stale cache groups, more-than-cap candidates, and a
query supplied only to the online branch.


| Concern | Contract |
|---|---|
| Filter score | Eligibility score is distinct from relevance score and has a declared direction. |
| Tie rule | Equal scores resolve by stable candidate identity, never physical row order. |
| Artifact key | Cached filters include query, policy, corpus, and source snapshot identity. |
| Cache validity | A cached group is usable only while policy and snapshot are effective. |
| Cap | Candidate limits are explicit and applied at the documented boundary. |
| Scope | Online query identity is isolated from offline artifact identity. |

Filtering is an admission boundary. It may reduce work, but it must not smuggle a ranking decision into the
filter score or make a stale cache appear current. When cache and live rows are combined, merge and
deduplication keys are part of the contract.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Filter location | Alternatives in choices above | Before expensive reranking | Keeps lineage explicit |
| Signal | Alternatives in choices above | Declared policy score | Keeps lineage explicit |
| Cache | None; global; identity-keyed artifact | Identity-keyed artifact | Reuse is safe only when lineage matches. |

Diagnostics should expose stale, future-dated, duplicate, and identity-mismatched rows. Evidence must include
ties, zero-term queries, cap overflow, stale artifacts, and a query available only to the live branch.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
