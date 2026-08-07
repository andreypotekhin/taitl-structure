# Search Example Reference

The Search example is a typed batch search pipeline over caller-provided document and interaction relations. Use this
page to find the transform for chunking, indexing, scoring, presentation, similarity, feedback, or evaluation.

The Search background defines why evidence boundaries remain separate, and the Search example guide describes the
end-to-end workflow. Search transforms own DataFrame transformations; callers own sources, persistence, serving,
streaming lifecycle, and checkpoints.

This page describes the bundled Search example. Its schemas and transform names are example-app APIs, not additional
Structure core operations. The source declarations live under `examples/search/schemas/` and
`examples/search/transforms/`; the behavior remains defined here so the reference is usable without navigation.

## Pipeline map

| Need | Transform family | Result |
| --- | --- | --- |
| Split document text | `Chunking`, `DocumentChunking`, `SentenceChunking`, `WordChunking` | Hierarchical text rows |
| Build reusable corpus statistics | `ProfileDocuments`, `AnalyzeText`, `CorpusText` | Features and corpus summaries |
| Build a lexical index | `Indexing` | Terms and summaries at four grains |
| Score a query batch | `Scoring`, `ScoreOverlap`, `ScoreBm25`, `SelectScores` | Overlap and BM25 score relations |
| Search sentences/passages/documents | `SearchSentences`, `SearchPassages`, `SearchDocuments` | Ranked results |
| Find similar corpus items | Similarity query, score, and reducer transforms | Ranked same-grain pairs |
| Turn interactions into evidence | `Impressions`, `Clicks`, `BuildRelevanceSignals` | Daily facts and signals |
| Measure quality and serving | evaluator transforms | Judged-quality and behavior summaries |

```python
chunks = Chunking(documents=documents).run(session)
index = Indexing(words=chunks.words).run(session)
scores = Scoring(
    queries=queries,
    document_terms=index.document_terms,
    document_summary=index.document_summary,
).run(session)
```

Each stage consumes a named typed relation from the previous boundary; the map is not a single hidden search action.

## Build artifacts

Use `All` when a batch workflow needs the reusable corpus, score, similarity, and feedback artifacts together.

```python
from examples.search.transforms.all import *

artifacts = All(
    documents=documents,
    similarity_policy=similarity_policy,
    score_policy=score_policy,
    queries=queries,
    query_labels=query_labels,
    intents=intents,
    patterns=patterns,
    daily_impressions=daily_impressions,
    daily_clicks=daily_clicks,
    users=users,
    bands=bands,
    policy=policy,
).run(session)

document_scores = artifacts.document_scores
```

`All` is a convenient batch relevance snapshot. It emits corpus extraction, lexical artifacts, similarity pairs,
feedback-derived signals, and score relations. It does not start a serving query, aggregate future events, train a
model, or persist artifacts; the caller handles those actions.

## Source contracts

Search transforms accept typed relations rather than a hidden corpus or query service. The most frequently used source
contracts are:

| Relation | Required meaning |
| --- | --- |
| `Document` | Stable document ID, source metadata, and plain-text content |
| `SearchQuery` | Query ID, queryset, free-form content, immutable `requested_at`, labels, and language flags |
| `SearchRequest` | One serving attempt, including no-result attempts and immutable `ranking_version` |
| `Impression` | One displayed document, one-based position, timestamp, and logged propensity |
| `Click` | One action linked to an impression, with occurrence time and dwell seconds |
| `ScorePolicy` | Score freshness, effective timestamp, and per-grain lexical weights |
| `RelevancePolicy` | Decay, lexical/feedback weights, dwell/CTR weights, and impression thresholds |

`SearchQuery.id` partitions score and rank output. `SearchQuery.queryset` identifies the query population, such as
`natural` or `synthetic`. Query text is normalized by the same rules used for document words, so equivalent text can
share lexical and feedback evidence while retaining separate request-local IDs.

`requested_at` is the event-time key used by serving and streaming compatibility. The matching
`SearchRequest.requested_at` must agree with it. A caller that changes query identity or event time must publish a new
request rather than mutating an existing query row.

Search outputs are evidence relations. They are safe to persist, page, compare, or pass to another system, but they do
not imply that the caller displayed a result, accepted an answer, or refreshed the corpus.

Keep query identity and serving-attempt identity separate:

```python
queries = SearchQuery.project(query_source)(
    id=query_source.id,
    queryset=query_source.queryset,
    content=query_source.content,
    requested_at=query_source.requested_at,
)

requests = SearchRequest.project(request_source)(
    id=request_source.id,
    query_id=queries.id,
    query=request_source.query,
    ranking_version=request_source.ranking_version,
    requested_at=request_source.requested_at,
)
```

The query can be scored once and reused, while each request remains an auditable serving attempt.

## Chunk and normalize text

`Chunking` accepts raw `Document` rows. A line beginning with `#` starts a section; blank lines separate paragraphs;
documents without a heading use an implicit document section. The pipeline emits a hierarchy of sections, paragraphs,
sentences, and normalized words.

```python
segments = Chunking(documents=documents).run(session)
words = segments.words
sentences = segments.sentences
```

The default sentence splitter is an explicit Python UDF starting point. It can split abbreviations, initials, versions,
domains, and locale-specific text incorrectly. Replace the sentence transform when source-faithful spans matter, then
feed its output to `WordChunking`.

## Index at the target grain

Run `Indexing` after text normalization when later scoring needs term statistics at each search grain.

```python
index = Indexing(words=segments.words).run(session)

document_terms = index.document_terms
document_summary = index.document_summary
sentence_terms = index.sentence_terms
sentence_summary = index.sentence_summary
```

Document, section, paragraph, and sentence indexes have independent term frequencies, document frequencies, and length
statistics. A score at one grain is not silently reused at another. The application persists index relations.

## Score queries

`Scoring` accepts `SearchQuery` rows with `id`, `queryset`, `content`, and immutable `requested_at`, plus the reusable
index. It emits score rows at document, section, paragraph, and sentence grains.

```text
overlap = sum(idf(matched_distinct_terms)) / sum(idf(query_distinct_terms))
BM25(k1 = 1.2, b = 0.75)
```

Query and document terms use the same lowercasing, whitespace, punctuation, and token normalization. Overlap is bounded
and symmetric at a fixed grain. BM25 is corpus-dependent and directional; neither score is a calibrated relevance
probability.

`SelectScores` combines overlap and normalized BM25 with `ScorePolicy` weights. Every score carries `scored_at` and
serving rejects rows older than the policy's `effective_at` when the policy changes.

### Score freshness

Cached score and filter rows are usable only when all of these conditions hold:

- `scored_at <= requested_at`;
- the age is no greater than `ScorePolicy.maximum_age_days`;
- `scored_at >= ScorePolicy.effective_at`;
- the experiment identity matches, using null-safe equality.

`effective_at` invalidates artifacts produced under an older policy. A timestamp alone does not make a score current;
the caller must resolve the policy and snapshot together. Online transforms fill missing or invalid query groups from
the same reusable index and do not silently mix incompatible score versions.

```python
fresh = scores.document_scores.where(
    (scores.document_scores.scored_at <= query.requested_at)
    & (scores.document_scores.scored_at >= policy.effective_at)
)
```

In production, use the Search freshness-aware transform contract rather than rebuilding this predicate ad hoc; the
example shows the timestamp relationships that must remain visible.

`ScorePolicy` has separate BM25 and overlap weights for documents, sections, paragraphs, and sentences. Keep the
weights explicit when comparing runs. A score row with a different experiment or policy timestamp is a different
evidence snapshot, not a duplicate to be merged casually.

## Present ranked results

Use presentation transforms after scoring when the application needs ranked sentences, passages, or documents.

```python
scores = Scoring(
    queries=queries,
    document_terms=index.document_terms,
    document_summary=index.document_summary,
    section_terms=index.section_terms,
    section_summary=index.section_summary,
    paragraph_terms=index.paragraph_terms,
    paragraph_summary=index.paragraph_summary,
    sentence_terms=index.sentence_terms,
    sentence_summary=index.sentence_summary,
).run(session)

sentences = SearchSentences(
    queries=queries,
    sentences=segments.sentences,
    sentence_scores=scores.sentence_scores,
).run(session)

ranked = sentences.results
```

Sentence and passage presentation ranks by score, overlap, and stable identifiers. `rank` is one-based;
DataFrames have no physical row order, so consumers should sort or page by query ID and rank.

`SearchDocuments` uses a staged funnel: retrieve a BM25 candidate set, narrow by overlap, then rerank with relevance
signals. Its current ranking and deduplication shape is batch-only; it does not expose a caller-adoption streaming
contract.

The concrete limits are class-level public constants:

| Transform | Constant | Default | Applied at |
| --- | --- | ---: | --- |
| `SelectFilterTargets` | `maximum_candidates` | `10000` | After cached and online filter rows merge |
| `RetrieveDocuments` | `maximum_candidates` | `1000` | After composite lexical retrieval |
| `RerankDocuments` | `maximum_results` | `100` | After feedback scoring and final ranking |

The implementation calls the retrieval setting `maximum_candidates`; `max_candidates` is not the documented
attribute. The limits are not interchangeable: feedback may reorder the 1,000 admitted lexical candidates, but it
cannot admit a document outside that set. A final top-100 result is therefore not the top 100 over the entire corpus.

The filter cap is a separate, earlier admission guard. Increasing or replacing one cap changes a distinct stage and
must not be described as changing the final result limit.

### Retrieval identity and fallback

Candidate identity includes query, experiment, user-band context, candidate rank, and document ID. The retrieval
stages preserve that identity while merging stored and online scores. Band-specific feedback falls back through the
configured band hierarchy and then to global feedback only when the minimum-impression policy is satisfied. A missing
feedback row contributes zero; it does not remove the lexical candidate.

`DocumentSearchResult` exposes both `candidate_rank` and final `rank`, plus `score`, `score_feedback`, and `score_rank`.
Use those fields to explain why a document moved. Do not reconstruct candidate rank from final rank after reranking.

```python
return DocumentSearchResult.project(ranked)(
    document_id=ranked.document_id,
    candidate_rank=ranked.candidate_rank,
    rank=ranked.rank,
    score=ranked.score,
    score_feedback=ranked.score_feedback,
    score_rank=ranked.score_rank,
)
```

Retaining both ranks lets a caller distinguish lexical admission from feedback-driven reordering.

## Similarity

`CreateSimilarityQueries` turns document, section, paragraph, and sentence vocabularies into tagged queries. Score those
queries with the shared lexical index, then call `ReduceSimilarityScores`.

The reducer returns at most 10 neighbors per source target and grain, ordered by source-to-candidate BM25, overlap, and
candidate identifiers. It retains both BM25 directions, their arithmetic mean, and the bounded overlap coefficient.
`bm25_mean` is useful for inspection, not a calibrated probability.

`Similarity`, `SimilarSections`, `SimilarParagraphs`, and `SimilarSentences` present a one-row query against same-grain
pairs. `SimilarityPolicy.max_document_frequency_ratio` explicitly controls common-token pruning; null retains every
normalized term.

```python
similarities = Similarities(
    policy=similarity_policy,
    score_policy=score_policy,
    document_terms=index.document_terms,
    document_summary=index.document_summary,
    section_terms=index.section_terms,
    section_summary=index.section_summary,
    paragraph_terms=index.paragraph_terms,
    paragraph_summary=index.paragraph_summary,
    sentence_terms=index.sentence_terms,
    sentence_summary=index.sentence_summary,
).run(session)
neighbors = similarities.document_similarities
```

Use the matching grain in each call; do not feed document statistics into a sentence-similarity reducer.

Similarity is a directed lexical comparison. The reducer keeps the source-to-candidate BM25 direction, the reverse
direction, their arithmetic mean, and the symmetric overlap coefficient. `bm25_mean` is a convenience for inspection;
it is not a calibrated probability. The default presentation cap is ten neighbors per source target and grain.

The same-grain rule matters: document pairs are compared with document statistics, section pairs with section
statistics, and so on. A document-level match is not a substitute for a sentence-level match. The similarity policy's
frequency-ratio setting is the explicit candidate-pruning control; the pipeline does not hide a common-token threshold.

## Sentence and passage presentation

`SearchSentences` accepts `SearchQuery`, immutable sentences, and `sentence_scores`. It returns one-based
`SentenceSearchResult` rows with query, document, section, paragraph, sentence, content, score, and experiment identity.
The ordering is descending BM25, descending overlap, document ID, and sentence ID.

`SearchPassages` accepts immutable paragraphs, sections, documents, and paragraph scores. Its
`PassageSearchResult` contains the matched paragraph plus same-section preceding and following content. Neighboring
paragraphs do not affect score or rank, and context becomes null at a section boundary. Use the emitted `rank` for
pagination; physical Spark row order is not a contract.

Both presentations return every matching row within their input score relation. A caller chooses the page size,
context budget, duplicate handling, and answer assembly. The example does not invoke a language model or build a
cross-document prompt.

```python
passages = SearchPassages(
    queries=queries,
    paragraphs=segments.paragraphs,
    sections=segments.sections,
    documents=documents,
    paragraph_scores=scores.paragraph_scores,
).run(session)

page = passages.results.where(passages.results.rank <= 10)
```

The page limit is a caller decision; the transform supplies ranked, bounded presentation rows and same-section context.

## Feedback and evaluation

Impressions record exposure; clicks are attributed only when they reference an impression within the documented window.
`BuildRelevanceSignals` creates exposure-aware, attributed, propensity-corrected evidence. CTR is descriptive behavior
evidence, not a relevance judgment or causal experiment result.

Evaluation accepts judged quality, query intent, user/band, and serving evidence. Use inclusive band hierarchies and
slice metrics by labels to avoid hiding performance differences in one aggregate. Experiment assignment and observed
variant summaries do not establish causal impact without an explicit exposure and selection-probability contract.

### Feedback event semantics

`Impressions` and `Clicks` are streaming-compatible transformations with application-provided sources and sinks. Both
event
IDs are watermark-deduplicated within seven days. An impression contributes exposure even without a click. A click
must reference an impression and occur from display time through 24 hours later; orphan, duplicate, late, and
out-of-window clicks produce no attributed daily click fact.

```python
daily_impressions = Impressions(impressions=impression_events).run(session).daily_impressions
daily_clicks = Clicks(
    impressions=impression_events,
    clicks=click_events,
).run(session).daily_clicks
```

Persist daily facts with an idempotent update sink keyed by the complete daily-fact identity. Structure does not own
checkpoints, triggers, output modes, or the sink. Events beyond the watermark may be discarded by the streaming
runtime.

### Relevance signals

`BuildRelevanceSignals` consumes persisted daily facts and exactly one `RelevancePolicy` row. It retains raw exposure,
click, dwell, long-click, and CTR fields alongside normalized ranking signals. A click count may exceed the
clicked-impression count because one impression can receive multiple clicks; CTR uses the binary clicked-impression
count.

The ranking evidence uses a 30-day-style decay configured by `half_life_days`, caps dwell credit at 60 seconds, and
uses the logged examination propensity for inverse-propensity correction:

```text
dwell_credit = clamp(dwell_seconds, 0, 60) / 60
age_weight = 2 ** (-max(age_days, 0) / half_life_days)
ips_credit = credit / examination_propensity * age_weight
```

The CTR component remains zero until the configured minimum impression threshold is met. The default signal blend is
70% normalized dwell and 30% normalized CTR. Document reranking then blends 80% query-document feedback and 20%
document-popularity feedback before applying the caller's lexical/feedback weights.

Propensity must be supplied by the serving system in `(0, 1]`; Search does not infer it from position. Feedback is
observed behavior evidence, not an offline relevance judgment.

### Evaluation contracts

`EvaluateDocumentRanking` is batch quality evaluation. Use one UTC-aligned `EvaluationBatch`, the complete query
population, one result run, and caller-supplied query-document judgments. Grades are `0` not relevant, `1` related,
`2` relevant, and `3` ideal. Grades 2 and 3 count as relevant for precision, recall, success, and reciprocal rank; all
four grades contribute to nDCG. A returned document without a judgment makes the affected metric unavailable rather
than silently treating it as irrelevant.

`EvaluateDocSearchBehavior` monitors observed requests, impressions, clicks, long clicks, no-result attempts, ranking
versions, and propensity-adjusted exposure. Its metrics describe serving behavior and must not be presented as judged
relevance or causal impact.

```python
signals = BuildRelevanceSignals(
    daily_impressions=daily_impressions,
    daily_clicks=daily_clicks,
    policy=relevance_policy,
).run(session)
```

Use the signal snapshot for reranking evidence, while keeping the evaluation transforms as separate quality and
behavior measurements.

Run each ranking candidate against the same batch, query population, and judgment pool. Persist an application run ID
beside each summary so comparisons remain tied to a specific corpus, policy, and ranking version.

```python
quality = EvaluateDocumentRanking(
    batch=evaluation_batch,
    queries=queries,
    results=ranked.results,
    judgments=query_judgments,
).run(session)

behavior = EvaluateDocSearchBehavior(
    requests=requests,
    impressions=impression_events,
    clicks=click_events,
).run(session)
```

Quality judgments and observed serving behavior are separate inputs and should be reported as separate evidence.

## Boundaries

- Callers own document sources, index persistence, query serving, writes, and streaming lifecycle.
- The example does not provide a web search server, crawler, model trainer, vector index, or causal inference system.
- Search transforms should remain explicit about whether a row is a source fact, candidate, score, exposure, judgment,
  or presentation result.
- A missing row can mean no match, suppression, no exposure, no judgment, or not-yet-observed evidence; do not collapse
  those meanings in a downstream join.
- Replace the default sentence splitter when exact source spans matter; its UDF is intentionally only a starting point.
- Refresh corpus and score snapshots explicitly; there is no hidden corpus cache or freshness scheduler.

```python
results = artifacts.document_scores
# The caller controls this write and any later serving action.
results.write.mode("overwrite").parquet(results_path)
```

The transform produces evidence rows; it does not publish a response or start a serving process.

## Artifact handoff

The most useful persisted handoff boundaries are:

| Artifact | Consumers |
| --- | --- |
| Extracted sections, paragraphs, sentences, words | Indexing, passage context, corpus analytics |
| Target-grain terms and summaries | Offline and online scoring, similarity queries |
| `DocumentFilterScore` and `DocumentSearchTarget` | Search-document candidate admission |
| `DocumentScore` and other grain scores | Sentence, passage, and document presentation |
| Similarity pairs | Same-grain related-content presentation |
| Daily impressions and attributed clicks | Relevance signal snapshots and behavior evaluation |
| Query-document and document-popularity signals | Document reranking and feedback analysis |

Each artifact should be tagged by the caller with its corpus snapshot, policy/effective timestamp, experiment identity,
and ranking version where applicable. Structure validates the typed fields and freshness rules described by the input
policy; it does not maintain a global artifact registry or select a storage location.

```python
scores.document_scores.withColumn("corpus_snapshot", lit(snapshot_id)).write.mode("overwrite").parquet(score_path)
daily_clicks.write.mode("append").parquet(feedback_path)
```

The storage format, overwrite policy, and snapshot lifecycle belong to the caller; the transform only produces the
typed relations.

## Offline and online responsibilities

Offline transforms are appropriate for corpus refresh, index construction, popular/recent query scoring, daily feedback
aggregation, similarity reduction, and judged evaluation. Online transforms fill request-specific gaps, select current
filter and score artifacts, rerank an already admitted candidate set, and present bounded result rows.

The online path must not silently rebuild the whole corpus or bypass the offline boundaries. `OnlineFiltering` and
`OnlineScoring` resolve missing or stale query groups from reusable indexes; they do not turn a request into an
unbounded full-corpus scan by implication. `RetrieveDocuments.maximum_candidates` and
`RerankDocuments.maximum_results` remain explicit safeguards.

```python
index = Indexing(words=words).run(session)
reusable_terms = index.document_terms
reusable_summary = index.document_summary
# Supply these artifacts to the declared request-scoring inputs.
```

The online stage reuses the offline index and operates on request-specific inputs rather than rebuilding the corpus.

## Before checking search quality

- Normalize query and document terms through the same token contract.
- Keep statistics independent for document, section, paragraph, and sentence grains.
- Carry `scored_at`, `effective_at`, `maximum_age_days`, and experiment identity with cached scores.
- Apply the 10,000 filter-target, 1,000 retrieval-candidate, and 100 final-result boundaries at their named stages.
- Preserve candidate rank when explaining feedback-driven movement.
- Keep unclicked impressions and no-result requests visible to feedback/evaluation.
- Log valid examination propensity at serving time; do not infer it from rank.
- Treat clicks as behavior evidence and judgments as offline quality evidence.
- Page and sort by emitted rank, never physical Spark row order.
- Keep sources, storage, query lifecycle, and answer generation under application control.

## Compatibility and operations

The lexical batch path is available through ordinary Structure execution and generated code when the selected target
admits its joins, windows, and collection helpers. The click aggregation transforms use the application-controlled
streaming
contract: watermarks and bounded deduplication are explicit, while query sources, sinks, checkpoints, and restart are
not owned by Search.

`SearchDocuments` currently remains batch-only. Its streaming-shaped inputs document a retained migration boundary, not
a permission to start a query. The deferred design requires finite event-time completion, bounded top-K state,
append-only results, and restart evidence before the transform can be promoted.

When a score or feedback snapshot changes, start a new application run with a new effective timestamp or ranking
version. Do not mix rows from different snapshots merely because their physical columns match. Persist the metadata
needed to reproduce a result: corpus snapshot, score policy, relevance policy, experiment, ranking version, and request
time.

```python
run_metadata = {
    "score_policy": score_policy_v2,
    "ranking_version": "v2",
    "effective_at": effective_at_v2,
}
```

Treat a policy or snapshot change as a new ranking run rather than mutating the metadata of an existing result set.

## Before serving results

Before serving document results, the caller should confirm:

- the corpus and index snapshots are mutually compatible;
- score rows satisfy freshness and effective-policy checks;
- filter, retrieval, and final-result caps are applied at their named stages;
- `candidate_rank` is retained for explanation and audit;
- the request and ranking version are immutable;
- impressions are recorded for every displayed result, including its propensity;
- no-result requests are retained for behavior evaluation;
- pagination uses query ID and emitted rank;
- the caller, not Search, controls the response, cache, and downstream write.

These practices keep retrieval reproducible: the same source snapshots, policies, target profile, and request time
should produce the same logical result and explainable candidate movement.

Record the chosen page and snapshot metadata with any served result set so later feedback can be attributed to the
ranking run that actually produced the displayed rows.

At minimum, serving metadata should identify the query, request time, corpus snapshot, ranking version, experiment, and
effective policy timestamps. Store it with the emitted rank and candidate rank; a document ID alone cannot explain why a
result was shown or whether later feedback belongs to this ranking run.

```python
page = ranked.where(ranked.rank <= 10)
page.withColumn("ranking_version", lit(request.ranking_version)).write.mode("append").parquet(served_path)
```

Persist the request and ranking metadata with the page before handing the rows to the caller's response layer.

An empty result is still a serving event: retain the query, request metadata, applied filters, and snapshot identifiers
so zero-result behavior can be evaluated separately from successful retrieval.

## Related concepts

The Search background explains evidence separation and caller ownership. The Search example guide groups the focused
boundary specifications. Transform and execution references describe general Structure vocabulary; this page applies
that vocabulary to Search without requiring those documents.
