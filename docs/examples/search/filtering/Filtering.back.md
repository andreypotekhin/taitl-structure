# Search Document Filtering


Filtering provides a cheap, reusable candidate boundary before composite lexical scoring and feedback reranking.


The filter counts distinct normalized query terms shared with each document. It orders by descending matched-term count,
then document ID, and publishes timestamped `DocumentFilterScore` rows. `Filtering` creates offline artifacts for a
selected query set; `OnlineFiltering` computes only query groups missing or invalid in the cache and reuses usable
stored rows.

`SelectFilterTargets` applies request-time validity rules and retains at most 10,000 document targets per query. A
filter artifact cannot be used when it is future-dated, older than the configured maximum age, or older than the
policy's effective timestamp. Online filtering also carries an optional pre-existing target scope so already-admitted
query groups can remain stable while new filter rows are merged.

## How it works

Filtering runs before retrieval to create a clear boundary and bound the expensive candidate set. IDF-weighted
overlap remains a scoring feature; the filter deliberately uses simple distinct-term counts rather than performing
post-retrieval overlap narrowing.


Offline and online artifacts share one schema, cache gaps are resolved without recomputing usable groups, ties are
deterministic, and target count never exceeds the declared cap.

The admission score is intentionally cheaper than lexical relevance:

```text
matched_terms(q, d) = |distinct_normalized_query_terms ∩ indexed_document_terms(d)|
```

Rows order by matched-term count descending and document ID ascending before the 10,000-target cap. This is a cost
boundary: cheap deterministic admission happens first, while IDF-weighted overlap is reserved for composite scoring
after admission. Separating the stages keeps candidate volume bounded while preserving the richer
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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Filter location | After rank; before retrieve; before costly work | Before reranking | Bound growth early. |
| Signal | Full relevance; raw count; policy score | Policy score | Filter stays cheap. |
| Cache | None; global; identity-keyed artifact | Identity-keyed artifact | Reuse is safe only when lineage matches. |


Diagnostics should expose stale, future-dated, duplicate, and identity-mismatched rows. Examples should include
ties, zero-term queries, cap overflow, stale artifacts, and a query available only to the live branch.
