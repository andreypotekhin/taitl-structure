# Search Example App

This example models a harvested document corpus as typed Structure schemas,
turns caller-extracted text into sections, paragraphs, sentences, and words. The example computes lexical search, corpus similarity, and impression-backed
document reranking. Structure owns data transformations; callers own data
sources, persistence, query serving, stream lifecycles, and checkpoints.

For the architecture, evidence boundaries, and ownership model, see the
[Search background](../../docs/background/Search.back.md).

## Pipeline map

| Concern | Typed boundary | Result | Design point |
| --- | --- | --- | --- |
| Extraction | `ExtractText` | sections, paragraphs, sentences, words | Plain-text hierarchy and shared token normalization. |
| Indexing | `CreateIndex` | target-grain terms and summaries | Build once; score many query batches. |
| Scoring | `ScoreAll`, `AddScores` | overlap and BM25 on corpus rows | Keep scoring algorithms separate. |
| Similarity | `CreateSimilarityQueries`, `ReduceSimilarityScores` | same-grain corpus pairs | Reuse the lexical index; BM25 stays directional. |
| Feedback | `Impressions`, `Clicks`, `BuildRelevanceSignals` | daily facts and batch signals | Exposure-aware, attributed, propensity-corrected evidence. |
| Presentation | `SearchSentences`, `SearchPassages`, `SearchDocuments` | deterministic ranks | Sentence and passage ranking are lexical; document ranking is two-stage. |

## Extraction

`Document.content` is plain text similar to MarkDown: a line beginning with `#` starts a
section and supplies its heading; blank lines separate paragraphs. Documents
without a heading use an implicit `Document` section.

`ExtractText` accepts raw `Document` rows and emits hierarchical sections, paragraphs, sentences, and normalized words. All later
lexical paths use that one text-normalization contract.

```python
segments = ExtractText(documents=documents).run(session)
features = ProfileDocuments(documents=documents).run(session).features
analytics = AnalyzeText(
    words=segments.words,
    sentences=segments.sentences,
    paragraphs=segments.paragraphs,
    sections=segments.sections,
    comparison_left=features,
    comparison_right=features,
).run(session)
corpus = CorpusText(documents=analytics.document_statistics, words=segments.words).run(session)

# Feed extracted relations into later transforms or persist as corpus artifacts.
words = segments.words
sentences = segments.sentences
document_statistics = corpus.corpus_statistics
```

`corpus.corpus_statistics` reports document-level averages and the distribution of document-average word length.
`corpus.corpus_vocabulary` independently estimates distinct corpus vocabulary; it never sums per-document vocabularies,
which would double-count shared terms.

## Indexing

`CreateIndex` builds reusable document, section, paragraph, and sentence index from the extracted words. Each
term row holds its target-local frequency and vocabulary/length facts plus the token's target-grain document frequency;
each summary holds target count and average target length. Its term and summary relations are reusable across query
batches; persisting them is caller-owned.

```python
index = CreateIndex(words=segments.words).run(session)

# Index artifacts for persisting.
document_terms = index.document_terms
document_summary = index.document_summary
sentence_terms = index.sentence_terms
sentence_summary = index.sentence_summary

# The index also drives paragraph and section scoring.
paragraph_terms = index.paragraph_terms
section_terms = index.section_terms
```

### Index grains

Document, section, paragraph, and sentence rows deliberately have independent term frequencies, document frequencies,
and length statistics. A document score is not silently reused as a sentence score. This permits corpus search and
passage-level presentation to share normalization without conflating their retrieval models.

## Keyword Search

`ScoreBase` declares the shared query and four target-grain index inputs. `ScoreOverlap` and `ScoreBm25` independently
inherit that base; `ScoreAll(ScoreOverlap, ScoreBm25)` composes both score families. `AddScores` joins their outputs
onto corpus rows. Together they accept a DataFrame conforming to `SearchQuery` and reusable indexes, emit separate
overlap/BM25 score lanes, and preserve the distinction between lexical scoring and result presentation.

`AddScores` accepts a caller-supplied DataFrame conforming to `SearchQuery` (`id` and `content`) plus matching index
artifacts and creates a score row for every document, section, paragraph, and sentence that shares a keyword with the
query. `content` is free-form text: callers do not pre-tokenize it. For example, `"  AURORA,   beacon! navigation?  "` is equivalent to
`"aurora beacon navigation"`.
The algorithms normalize query terms exactly as
`ExtractText` normalizes document words. `score_overlap` is the standard overlap coefficient: matching distinct terms
divided by the smaller of the query and target vocabularies. `score_bm25` uses fixed `k1=1.2` and `b=0.75` constants.
The scores remain separate: choosing an algorithm or combining parent and child targets is deliberately caller-owned.

`SearchQuery.id` is the request-local key used to partition scores and ranks; one invocation can contain many query
rows. Query text is normalized with the same lowercasing, whitespace, punctuation, and token rules as extraction.
This is also the feedback join key, letting equivalent searches share evidence across request IDs.

```text
overlap = matching_distinct_terms / min(query_distinct_terms, target_distinct_terms)
BM25(k1 = 1.2, b = 0.75)
```

### Score semantics

Overlap is bounded and symmetric at a fixed grain. BM25 is corpus-dependent and directional when used for similarity;
do not interpret either score as a calibrated relevance probability.

```python
scores = AddScores(
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

# Choose the target grain needed by the next search boundary.
scored_documents = scores.scored_documents
scored_sentences = scores.scored_sentences
```

## Similarity Search

### Funnel

1. Turn corpus vocabularies into similarity queries.
2. Score queries with the shared lexical index.
3. Reduce directed scores to one pair per target pair.

### Build and score similarity queries

`CreateSimilarityQueries` and `ReduceSimilarityScores` reuse those same artifacts for corpus self-similarity. The first
turns each document, section, paragraph, and sentence vocabulary into a tagged query; pass the combined query rows to
`ScoreAll`, then give its directed score rows to the reducer. The reducer returns at most 10 neighbors per source target
and grain, ordered by descending source-to-candidate BM25, descending overlap, and candidate identifiers. Each directed
pair retains the bounded symmetric overlap coefficient, both BM25 directions, and their arithmetic mean. BM25 remains
directional and corpus-dependent, so `bm25_mean` is a convenience for inspection rather than a calibrated probability.

### Reduce and present matches

**Inputs and outputs**

- `CreateSimilarityQueries` emits tagged queries and their source target rows.
- `ReduceSimilarityScores` emits ranked document, section, paragraph, and sentence neighbor relations, bounded to 10
  rows per source target.
- `Similarity` and the focused section/paragraph/sentence transforms emit top-ranked lookup results.

`SimilarityPolicy.max_document_frequency_ratio` is a one-row, caller-supplied optional candidate-pruning setting.
`null` retains every normalized term; a value in `(0, 1]` excludes terms occurring in more than that fraction of targets
at each grain. This controls common-token candidate growth without imposing a hidden threshold. The similarity family
does not require title, source, language, or collection matches; callers apply those business filters after scoring.

`Similarity` turns document-pair results into a query-document lookup. Supply a one-row `Document` DataFrame, the corpus
`Document` rows, and `ReduceSimilarityScores.document_similarities`; it returns up to its fixed `maximum_results` (10 by
default). Results rank by the query-to-candidate BM25 direction, then overlap and document id. Each `IndexedSimilarDocument`
preserves corpus metadata and sets `search_query_id` to the query document id. `SimilarSections`, `SimilarParagraphs`, and
`SimilarSentences` apply the same ranking rule to a one-row section, paragraph, or sentence and their corresponding
same-grain similarity pairs.

```python
similarity_queries = CreateSimilarityQueries(
    policy=similarity_policy,
    document_terms=index.document_terms,
    document_summary=index.document_summary,
    section_terms=index.section_terms,
    section_summary=index.section_summary,
    paragraph_terms=index.paragraph_terms,
    paragraph_summary=index.paragraph_summary,
    sentence_terms=index.sentence_terms,
    sentence_summary=index.sentence_summary,
).run(session)

directed = ScoreAll(
    queries=similarity_queries.queries,
    document_terms=index.document_terms,
    document_summary=index.document_summary,
    section_terms=index.section_terms,
    section_summary=index.section_summary,
    paragraph_terms=index.paragraph_terms,
    paragraph_summary=index.paragraph_summary,
    sentence_terms=index.sentence_terms,
    sentence_summary=index.sentence_summary,
).run(session)

similarities = ReduceSimilarityScores(
    document_queries=similarity_queries.document_queries,
    section_queries=similarity_queries.section_queries,
    paragraph_queries=similarity_queries.paragraph_queries,
    sentence_queries=similarity_queries.sentence_queries,
    document_overlap_scores=directed.document_overlap_scores,
    section_overlap_scores=directed.section_overlap_scores,
    paragraph_overlap_scores=directed.paragraph_overlap_scores,
    sentence_overlap_scores=directed.sentence_overlap_scores,
    document_bm25_scores=directed.document_bm25_scores,
    section_bm25_scores=directed.section_bm25_scores,
    paragraph_bm25_scores=directed.paragraph_bm25_scores,
    sentence_bm25_scores=directed.sentence_bm25_scores,
).run(session)

# Reuse a same-grain pair set for the corresponding presentation transform.
document_pairs = similarities.document_similarities
```

## Sentence Search

The shared `AddScores` output supplies the sentence candidates. `SearchSentences`
accepts queries and `AddScores.scored_sentences`, emits one-based `SentenceSearchResult` ranks per query.

```python
index = CreateIndex(words=segments.words).run(session)
scores = AddScores(
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
overlap = scores.document_overlap_scores
bm25 = scores.document_bm25_scores
```

`SearchSentences` accepts a caller-supplied DataFrame conforming to `SearchQuery` and the scored output from
`AddScores`, and returns matching sentences as a `SentenceSearchResult`.
`rank` is a one-based, deterministic ordering by BM25, overlap, document ID, and sentence ID; Spark DataFrames do not
promise physical row order, so consumers sort or page by `rank`.

```python
result = SearchSentences(
    queries=queries,
    scored_sentences=scores.scored_sentences,
).run(session)
ranked_sentences = result.results
first_sentences = ranked_sentences.where("rank <= 20").orderBy("search_query_id", "rank")
```

## Document Search

### Funnel

1. Record displayed documents as impressions and user actions as clicks.
2. Produce daily facts and attribute clicks to impressions.
3. Build a decayed relevance snapshot.
4. Retrieve the BM25 top 100, rerank that candidate set with relevance.

### Record user feedback

Feedback events:

- `Impression`: `id`, `shown_at`, query, document ID, displayed position, and calibrated propensity.
- `Click`: `id`, `occurred_at`, impression ID, and dwell seconds.
- click must correspond to one impression; clicks later than 24 hours are not attributable.

`Impression` records one document result shown to a user, including a unique ID, the serving system's calibrated
`examination_propensity`, query text, document ID, displayed one-based position, and display timestamp. `Click` has its
own unique ID and references that immutable impression. IDs make watermark-bounded deduplication reliable rather than
guessing whether a repeated timestamp or dwell value is a transport retry.

### Aggregate the feedback

**Streaming guarantees**

- Both event IDs are deduplicated within a seven-day watermark.
- Impressions contribute exposure counts even when no click arrives.
- Orphan, duplicate, late, and out-of-window clicks produce no daily click fact.

`Impressions` watermarks `shown_at` by seven days, deduplicates IDs, normalizes the query, and publishes daily exposure
facts—even for unclicked results. `Clicks` watermarks and deduplicates both streams, attributes a click through an
inner stream-stream join on impression ID, and accepts only clicks from display time through 24 hours later. Orphan,
duplicate, late, and out-of-window clicks therefore create no attributed daily fact. The caller owns sources,
checkpoints, and idempotent `update` sinks keyed by the complete daily fact key; events beyond the seven-day watermark
may be discarded.

```python
# Inputs are streaming DataFrames.
daily_impressions = Impressions(
    impressions=impression_events
).run(session).daily_impressions
daily_clicks = Clicks(
    impressions=impression_events,
    clicks=click_events,
).run(session).daily_clicks

# Write results in update mode to durable, idempotent daily tables.
# daily_impressions.writeStream.outputMode("update").toTable("search_daily_impressions")
# daily_clicks.writeStream.outputMode("update").toTable("search_daily_clicks")
```

### Build relevance snapshot

`BuildRelevanceSignals` reads persisted daily facts and applies a 30-day exponential decay. It retains impressions,
raw clicks, binary clicked-impression counts, raw dwell seconds, long clicks, and CTR for observability. A click is an
engagement event; CTR is the probability that an impression received one or more attributed clicks. Consequently,
several clicks on one impression increase `click_count` and dwell, but increase the CTR numerator only once.

Ranking evidence uses a capped dwell credit and the logged inverse propensity:

```text
dwell_credit = clamp(dwell_seconds, 0, 60) / 60
age_weight = 2 ** (-max(age_days, 0) / half_life_days)
ips_credit = credit / examination_propensity * age_weight
```

At the query-document and document-only grains, the snapshot exposes raw exposure/click/dwell metrics, binary CTR,
IPS CTR, `ips_clicks`, and `ips_dwell_credit`. IPS CTR is a self-normalized estimate, using the logged propensity on
both sides of the ratio:

```text
raw_ctr = clicked_impression_count / impression_count
ips_ctr = sum(clicked_impression_count * age_weight / propensity)
          / sum(impression_count * age_weight / propensity)
```

The 30% CTR component is deliberately zero until the relevant grain has at least 20 impressions. This prevents a
single favorable exposure from dominating feedback, while dwell remains eligible immediately. IPS dwell is
`log1p`-scaled and normalized at its query-document or global-document grain. The final signal is
`0.7 * normalized_dwell_score + 0.3 * normalized_ctr_score`; document feedback then keeps the 80/20 query/global
mix. The serving policy or experiment must emit a calibrated propensity in `(0, 1]` with every impression; this
example does not infer it. The example policy defaults are a 30-day half-life, 70/30 BM25/feedback reranking,
70/30 dwell/CTR signal blending, and a 20-impression CTR threshold.

```python
signals = BuildRelevanceSignals(
    daily_impressions=persisted_daily_impressions,
    daily_clicks=persisted_daily_clicks,
    policy=policy,  # Exactly one RelevancePolicy row.
).run(session)

# Persist or cache the snapshot; document ranking uses these two relations as inpunt.
query_document_signals = signals.query_document_signals
document_popularity = signals.document_popularity
```

### Retrieve and rerank documents

`SearchDocuments` transform subclasses both `RetrieveDocuments` and `RerankDocuments`, which puts  them into a  two-stage ranking process. `RetrieveDocuments` creates a candidate lane by selecting  100
documents per query by descending BM25. `RerankDocuments` consumes that lane, joins user click feedback, and
emits results.

Feedback combines 80% of the normalized query-document signal with 20% global document popularity. Within each BM25
candidate set, the reranker calculates:

```text
normalized_bm25 = score_bm25 / max(score_bm25)
feedback = 0.8 * query_document_score + 0.2 * document_popularity_score
rank_score = policy.bm25_weight * normalized_bm25 + policy.feedback_weight * feedback
```

The fixture policy uses a 30-day half-life and 70/30 lexical-feedback weights. Documents without feedback remain
eligible with zero feedback. Final rank is descending `rank_score`, then document ID. A candidate outside the BM25 top
100 cannot enter through feedback, and a no-history query preserves BM25 order.

`SearchDocuments` uses the same free-form `SearchQuery` DataFrame and `AddScores.scored_documents` keyword matches as
the other search boundaries. Its additional inputs only rerank those lexical candidates; they do not change query
parsing.

```python
ranked_documents = SearchDocuments(
    queries=queries,
    scored_documents=scores.scored_documents,
    query_document_signals=signals.query_document_signals,
    document_popularity=signals.document_popularity,
    policy=policy,
).run(session).results

# Serve or page by rank; never rely on physical Spark row order.
first_page = ranked_documents.where("rank <= 20").orderBy("rank")
```

`DocumentSearchResult` exposes candidate rank, final rank, BM25, feedback, and final rank score so a serving layer can
explain movement without reconstructing the scoring path.

## Evaluation

`EvaluateDocumentRankingQuality` is the offline quality anchor. It compares one daily result batch with caller-supplied
four-grade query-document judgments (`0` not relevant, `1` related, `2` relevant, `3` ideal). It reports
nDCG, precision, judged recall, success, and reciprocal-rank metrics at 5, 10, and 15. Clicks are not used as
judgments. Returned documents without a judgment make the affected metric unavailable rather than silently wrong.

`EvaluateDocumentSearchBehavior` is the separate automated daily monitor. Emit one `SearchRequest` for every
serving attempt, including no-result attempts, and link every displayed `Impression` through
`search_request_id`. A request owns its immutable `ranking_version`. The transform reports request-level and
daily version-level no-result, click, long-click, first-satisfying-rank, and propensity-adjusted exposure metrics.
These are observed engagement signals, not relevance claims.

### Evaluate judged ranking quality

Evaluation is caller-owned and batch-only. The caller persists a shared judgment pool before comparing rankings:
for each query, collect the candidate documents from every ranking run being compared, deduplicate query-document
pairs, and assign a grade independent of rank. The four grades are `0` (not relevant), `1` (related), `2`
(relevant), and `3` (ideal). Grades 2 and 3 are binary relevant for precision, recall, success, and reciprocal rank;
all four grades contribute to nDCG.

`EvaluationBatch` contains exactly one UTC-aligned daily `TimeWindow`. `queries` is the complete query population,
so a query with no returned documents remains visible. `results` is one ranking run's `DocumentSearchResult` rows.
Run the transform once per candidate or baseline against the same `batch`, `queries`, and `judgments`, then persist a
caller-owned run identifier beside the summary.

```python
from examples.search.transforms.evaluate import EvaluateDocumentRankingQuality

quality = EvaluateDocumentRankingQuality(
    batch=evaluation_batch,          # One EvaluationBatch row.
    queries=queries,                 # Every query evaluated that day.
    results=ranked_documents,        # One ranking run.
    judgments=document_judgments,    # DocumentRelevanceJudgment rows.
).run(session)

per_query_quality = quality.query_evaluations
daily_quality = quality.summary
latest_quality = daily_quality.orderBy("window.end")
```

The per-query output has Precision, judged Recall, Success, and nDCG at 5, 10, and 15 plus reciprocal rank. Missing
result positions contribute zero gain. A returned unjudged document makes the affected metric null, rather than
silently treating an unknown document as irrelevant. The daily summary includes eligible-query counts, so a consumer
can distinguish a real quality change from weaker judgment coverage.

### Monitor served-result behavior

Behavior evaluation consumes the raw events from the serving system, not the feedback daily aggregates. Emit one
`SearchRequest` even for a no-result response. Every displayed document gets an `Impression` whose
`search_request_id` points to that request; each click points to its `impression_id`. The request's
`ranking_version` is required so a version change does not blend behavior into one historical series. Keep click data
through 24 hours after the selected request day because the transform attributes only clicks in that interval.

```python
from examples.search.transforms.evaluate import EvaluateDocumentSearchBehavior

behavior = EvaluateDocumentSearchBehavior(
    batch=evaluation_batch,  # The same one-day EvaluationBatch contract.
    requests=search_requests,
    impressions=impressions,
    clicks=clicks_through_next_day,
).run(session)

per_request_behavior = behavior.request_behaviors
daily_behavior_by_version = behavior.daily_behavior
latest_behavior = daily_behavior_by_version.orderBy("window.end", "ranking_version")
```

The request output makes a zero-result request, a no-click request, and a long-clicked request explicit. For each
request, the first long-click rank is the first displayed result receiving a click with at least ten seconds dwell;
its reciprocal is zero when no long click occurs. The daily output groups by ranking version and reports raw request
funnel counts plus inverse-propensity-weighted long-click and dwell-credit exposure rates. These rates help monitor a
served experience; they do not establish relevance or replace the judged-quality flow above.

## Passage Search

Passage Search can be used as a foundation for quesion-answer search engine.

`SearchPassages` ranks the scored paragraph and holds its immediate preceding and trailing paragraphs as context.
It returns `PassageSearchResult` rows with the document title and URL for citations, the section heading, and `preceding_content` and `following_content` if they exist. Context stays within a document section -  no heading transitions. The adjacent paragraphs do not affect lexical relevance or rank.
It uses the same free-form `SearchQuery` DataFrame and `AddScores.scored_paragraphs` keyword matches as sentence and
document search.

```python
passages = SearchPassages(
    queries=queries,
    scored_paragraphs=scores.scored_paragraphs,
    paragraphs=segments.paragraphs,
    sections=segments.sections,
    documents=documents,
).run(session).results

# Pick a ranked top-K, then let the answering layer combine or deduplicate contexts.
answer_evidence = passages.where("rank <= 5").orderBy("search_query_id", "rank")
```

`SearchPassages` returns every matching paragraph: adjacent matches remain distinct rows. Callers own refreshing the corpus and index, selecting a current snapshot, and turning
these evidence outputs into an answer; this example neither invokes an answer model nor creates a cross-document prompt.

## Design constraints

- The corpus and relevance snapshots are batch inputs, because similarity distributions and decayed normalization need bounded
  input sets.
- Document search candidate set is BM25-only. This prevents popularity from promoting unrelated documents.
- All rank orders have explicit identifier tie-breakers. Consumers must paginate by emitted rank, not DataFrame order.
- Similarity flow uses title prefix, source, and language as a candidate block before measuring the distance, avoiding
  an unrestricted self-Cartesian join.
