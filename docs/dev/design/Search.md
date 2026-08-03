# Design: Search Example

## Purpose

The Search example demonstrates how Structure can turn a caller-owned document corpus into typed, reproducible search
evidence. It supports lexical retrieval at several text grains, feedback-assisted document reranking, corpus
similarity, and two complementary evaluation facets. It is not an answer service, crawler, persistence layer, or
streaming-job framework.

The design prioritizes honest evidence boundaries. Lexical scores describe token match. Click-derived signals describe
observed interaction with a served result list. Explicit relevance judgments describe offline retrieval quality. None of
those evidence sources silently becomes another.

## Ownership and Scope

Structure owns the typed transformations between caller-provided DataFrames. The caller owns document harvesting,
source validation, persistence, index-refresh scheduling, query serving, streaming sources and sinks, checkpoints,
ranking-version deployment, and answer generation.

The current corpus is the corpus snapshot supplied to chunking, indexing, and scoring. A caller refreshes the corpus
by replacing those inputs and persisted artifacts; Search has no hidden corpus cache or freshness policy.

The example deliberately does not invoke a language model, synthesize an answer, collect driver-side corpus data, or
assemble a cross-document prompt. Its outputs are transparent evidence rows that a caller may cite, page, evaluate, or
pass to another system.

## Text Model and Lexical Pipeline

`Document.content` is plain text. `Chunking` turns heading lines into sections and blank-line groups into paragraphs. Its default sentence supplier is an explicitly declared punctuation-based Python UDF; it is a replaceable starting point, not a source-faithful segmenter. Callers that require exact sentence text or spans supply a `Paragraph`-to-`Sentence` transform and then reuse `WordChunking`. Words are normalized once for every later lexical path. The hierarchy preserves document,
section, paragraph, sentence, and word identifiers plus deterministic ordinals.

`Indexing` produces independent document, section, paragraph, and sentence index artifacts. Each grain has its own
term frequency, document frequency, target length, vocabulary size, target count, and average length. A score at one
grain must never be reused as a score at another grain.

`SearchQuery.id` is the request-local partition key for scores and ranks. Query text is normalized with the same token
rules used for extraction. The normalized query text is also the feedback aggregation key, allowing separately issued
equivalent requests to share historical evidence without confusing their request-local result ranks.
`SearchQuery.queryset` is a required caller-defined collection name, for example `natural` or `synthetic`, so evaluation
can slice comparable ranking runs by query source.

`ScoreOverlap` exposes a bounded lexical-overlap score. `ScoreBm25` exposes BM25 with fixed example parameters
`k1 = 1.2` and `b = 0.75`. They remain separate score lanes: a caller or focused presentation transform chooses how to
use them. Neither is a calibrated relevance probability.

Every unified score row also carries `scored_at`. `ScorePolicy` supplies the score snapshot timestamp and its maximum
serving age. `OfflineScoring` aggregates daily impression volume by normalized query, scores the most popular
configured number of queries, and adds every query observed in the preceding seven days. This keeps disk use practical
while covering the current query population and leaving arbitrary older production queries to online resolution.

## Search Presentations

### Sentences

`SearchSentences` returns every matching sentence for one or more query rows. It ranks each query by descending BM25,
descending overlap, document ID, and sentence ID. Consumers page by emitted rank rather than physical DataFrame order.

### Passages

`SearchPassages` uses extracted paragraphs as the initial passage grain. A result contains the matched paragraph, its
document title and URL, section heading, lexical scores, and nullable preceding and following paragraph content.
Context is calculated only within the same document section, so it cannot cross a heading boundary.

Only the matched paragraph contributes to rank. Neighboring paragraphs are answer context, not additional retrieval
terms. Adjacent matched paragraphs remain distinct ranked rows; callers choose their own top-K, overlap handling, and
prompt assembly. This preserves lexical evidence and avoids imposing an answer-model policy on a search example.

### Documents

`SearchDocuments` first runs `OnlineScoring`. It treats caller-supplied document and overlap score relations as
cache-compatible snapshots, discards rows older than `ScorePolicy.maximum_age_days` (or newer than the request), and
calculates missing query groups from the reusable indexes. The newly calculated rows are exposed as additional score
outputs; retrieval unions them with caller-supplied stored and streamed rows, so a caller can persist those rows and
reuse them on a later request. Score relations remain the cache contract—there are no parallel cache schemas, query-key,
or index-version fields.

The resulting three-stage document path admits up to 1000 persisted or streamed candidates per query using descending
score and document ID as the deterministic tie-breaker. It then filters to 100 candidates by overlap score before
enriching only those candidates with feedback and ranking by the final combined score. A document outside the lexical
candidate set cannot enter through popularity or click history. Documents without feedback remain eligible with zero
feedback.

The feedback score combines query-document evidence and document-wide popularity. Within each candidate set, BM25 is
normalized by that query's maximum candidate BM25. The final score blends normalized BM25 and feedback with the
caller-supplied policy weights. Overlap is used only as the explicit candidate-narrowing boundary.

### Streaming query boundary

`SearchDocuments.queries` is a declared streaming input. That declaration is propagated through `OnlineScoring`, gap
selection, `Scoring`/`ScoreBase`, `RetrieveDocuments`, `OverlapDocuments`, and `RerankDocuments`; the compiler therefore
keeps query-derived scores, candidates, and results in the same streaming lineage. The corpus, lexical indexes,
freshness policy, feedback snapshots, and ranking policy remain caller-supplied side inputs. Offline `All` and its
query-producing stages remain batch-only.

The current graph is a compiler-visible migration boundary, not yet a ready-to-start Structured Streaming query. The
implementation roadmap is recorded in
[`P08022605.SearchDocuments-structured-streaming.plan.md`](../planning/P08022605.SearchDocuments-structured-streaming.plan.md).
The required end state is append-only: one final result set per query after a finite event-time completion window, with
no later revisions. Query and request events use immutable matching `requested_at` timestamps and caller-configured
watermark delay; indexes, score caches, feedback, and policy relations are immutable static snapshots for one run.

The migration must remove global query-term and score-cache deduplication, replace static/streaming unions with
streaming branches followed by exact-schema stream/stream unions, and replace candidate/overlap `row_number` windows
with bounded finite-window top-K state. Feedback fallback and popularity selection must be pre-resolved in the static
snapshot, leaving only stream/static lookups and bounded normalization in the query path. Any remaining unbounded state,
global analytic window, unsupported stream-stream join, or arbitrary-state API remains rejected before query start.

Structure still owns only DataFrame transformations. The caller owns sources, watermarks, checkpoints, triggers, output
sinks, snapshot refreshes, restart policy, and downstream materialization. A snapshot refresh starts a new run; emitted
append-only results are never revised.

## Feedback Evidence

An `Impression` records one displayed document: immutable impression ID, parent request ID, display time, raw query,
document ID, displayed position, and calibrated examination propensity. A `Click` has its own ID, occurrence time,
impression ID, and dwell duration. A `SearchRequest` is emitted for every search attempt, including a no-result attempt,
and carries the immutable request ID, request time, raw query, and ranking version.

The feedback flow has two stages:

1. Streaming transforms watermark and deduplicate events, then publish daily impression and attributed-click facts.
   Clicks must refer to an impression and occur from display time through 24 hours later. Orphan, duplicate, late, and
   out-of-window clicks do not produce an attributed fact. Impressions remain in the daily output even when unclicked.
2. A batch transform consumes persisted daily facts and a one-row relevance policy to build query-document and
   document-wide relevance snapshots. It applies recency decay, logged inverse-propensity correction, capped dwell
   credit, and independent normalization at each evidence grain.

The serving system, not Search, must calculate and log valid propensity for every impression. Position alone is not a
propensity model. Binary clicked-impression counts and raw click counts remain separate because several clicks can arise
from one displayed impression. Dwell is capped for ranking credit while raw dwell remains observable.

Feedback is ranking evidence, not relevance truth. It can reflect rank position, interface design, traffic mix, and user
intent as well as result quality. The feedback path is therefore never used to claim corpus recall or offline precision.

### CTR and engagement semantics

CTR means the probability that a displayed impression received at least one attributed click, not the number of click
events per impression. `DailyClicks` therefore carries both `click_count` and `clicked_impression_count`. Repeated
clicks on one impression contribute to raw engagement, dwell, and long-click diagnostics, but contribute one unit at
most to CTR. This makes CTR bounded by one and preserves the separate product question answered by repeated clicks:
how much a result was engaged with after it was opened.

Attributed clicks are grouped by the displayed impression's one-day window, not the click's calendar day. A click at
00:05 that follows a 23:55 impression belongs to the exposure that caused it. The daily click and impression facts
therefore share a stable complete key: display window, normalized query, document, displayed position, and logged
propensity.

### Relevance policy and score composition

The batch snapshot applies the same age weight and logged propensity to shown and clicked impressions. For either the
query-document or global-document grain:

```text
age_weight = 2 ** (-max(age_days, 0) / half_life_days)
ips_ctr = sum(clicked_impressions * age_weight / propensity)
          / sum(impressions * age_weight / propensity)
normalized_dwell = log1p(ips_dwell) / max(log1p(ips_dwell))
signal = dwell_feedback_weight * normalized_dwell + ctr_feedback_weight * ips_ctr
```

The CTR component is set to zero until that grain has `minimum_ctr_impressions` exposures. The default policy uses a
30-day half-life, 70/30 dwell/CTR signal blend, and a 20-impression threshold. Dwell remains eligible below the
threshold; the threshold deliberately stabilizes only the volatile binary-rate component. The document reranker keeps
its independent composition: 80% query-document signal plus 20% document popularity, then caller-supplied BM25 and
feedback weights (the example defaults are 70/30).

The self-normalized IPS ratio corrects for logged exposure policy only to the degree that those propensities are
calibrated. It is not an estimate of causal relevance, an invitation to divide by an arbitrary position curve, or a
replacement for explicit judged evaluation.

## Similarity

Similarity reuses the lexical index rather than a separate embedding system. It creates a query from each target's
vocabulary, scores it at the same text grain, and reduces directed scores into bounded same-grain neighbors. The output
keeps overlap, both BM25 directions, and their mean for inspection. BM25 remains directional and corpus-dependent;
its mean is a convenience value, not a probability.

Candidate pruning may exclude terms above a caller-provided maximum document-frequency ratio. Similarity does not apply
hidden title, source, language, or collection filters. Callers add those product constraints after scoring.

## Evaluation

Search evaluation has two separate facets that share a caller-selected UTC-aligned daily `EvaluationBatch` window.

### Cohort bands

Caller-owned user profiles can match several persisted `Cohort` predicates. Search derives a reusable ordered
`Band` from their most-specific matches rather than selecting or blending one band. `UserBand` maps each user to its
shared band. Band feedback falls back
through the lowest-priority band first and then to global feedback when it lacks enough impressions. The hierarchy
resolver is currently a narrow raw Spark boundary because the DSL has no recursive-relation operation; ordinary
feedback, ranking, and evaluation remain typed transforms. User-band evaluation selects contexts containing its
requested persisted band, while combined evaluation applies both that membership filter and query-label filters.

### Query intents

Caller-owned intent catalogs map stable intent IDs to English label names. `SearchQuery.language` holds a caller locale,
such as `en_UK`, and falls back to `en_US` when null. One-pattern `IntentPattern` rows map an intent and locale to a
regular expression. `CreateQueryLabels` creates binary label maps, and `MergeQueryLabels` overlays them after caller
labels. `Labeling` composes both stages as the Search app labeling pipeline. This makes multilingual intent slices
reproducible without claiming language understanding, relevance, or a ranking effect.

`EvaluationParams.queryset` optionally narrows evaluation to one `SearchQuery.queryset`; null keeps all query sets in the
same batch.

### Judged document quality

`EvaluateDocumentRanking` evaluates one document-ranking run against caller-supplied `DocumentRelevanceJudgment` rows. Grades
are 0 (not relevant), 1 (related), 2 (relevant), and 3 (ideal). Grades 2 and 3 are binary relevant; every grade affects
nDCG. The evaluator publishes per-query and daily Precision, judged Recall, Success, nDCG, and reciprocal-rank metrics
at cutoffs 5, 10, and 15 where applicable.

Judgments must cover returned documents for a metric to be available. An unjudged result is not silently treated as
nonrelevant, because that would bias comparison against a ranking run that returns a novel document. Short result lists
contribute zero gain for missing ranks. Compare ranking runs by evaluating each one against the same persisted judgment
pool; the generic result schema does not carry a ranker identifier.

### Request-aware behavior

`EvaluateDocSearchBehavior` measures interaction with the actual served list. It preserves no-result requests,
attributes clicks to displayed impressions within the 24-hour interval, and publishes per-request behavior plus daily
summaries by ranking version.

Request-level facts include result, clicked-result, and long-clicked-result counts; click and long-click flags; first
click and first long-click rank; and reciprocal first-long-click rank. Daily facts retain raw engagement counts plus
inverse-propensity-weighted long-click and dwell-credit exposure rates. A long click has dwell time of at least ten
seconds.

These are observed-satisfaction metrics. They must not be named Precision, Recall, MRR, or relevance quality, because
they measure the served experience rather than a relevance judgment.

## Compiler-visible Boundary

Ordinary search logic belongs in Structure step methods so it remains typed, traceable, explainable, and equivalent in
online and generated PySpark execution. Passage context uses typed `lag` and `lead` steps. Document BM25 normalization
uses a typed full-partition `window_max` step.

Raw boundaries remain only where the DSL cannot yet express the necessary relation semantics:

- document chunking uses typed row expansion and hierarchical parsing; its deliberately replaceable default sentence supplier is a declared Python UDF, because exact source-faithful sentence spans are caller-owned;
- lexical scoring needs query-token row expansion;
- index summaries need a global aggregate with a defined empty-corpus result;
- similarity-query creation needs sorted token collection and exact-one policy validation; and
- similarity reduction needs self-alias joins and canonical/reversed relation union;
- relevance-context expansion needs branchable typed union for scoped and global contexts;
- document reranking needs first-qualified priority selection over declared business keys; and
- cohort-band resolution needs relation assertions, bounded parent hierarchy, and deterministic fallback expansion.

These are capability gaps, not permission to hide ordinary logic in hooks. When a capability gains an explicit typed
contract, IR representation, target checks, generated rendering, and online/generated parity coverage, migrate the
narrowest corresponding raw boundary to steps.

## Deferred Work

The following work is explicitly outside this design slice:

- adaptive passage chunking and caller-configurable context radii;
- embeddings, vector search, answer-model invocation, and cross-document answer-context assembly;
- ERR, which the existing four-grade judgment contract can support later without migration;
- Accuracy@N, because the proposed definition duplicates Precision@N;
- experiment comparison and interleaving, which require experiment-arm and displayed-result-ownership facts;
- counterfactual policy evaluation, which requires logged randomized selection probabilities; and
- session reformulation metrics, which require a stable session identifier.
- propensity calibration, clipping, drift monitoring, and experiment assignment validation, which are serving-system
  responsibilities rather than transformations over already-logged facts;
- impression-level fraud, bot, and accidental-click classification, which requires identity and trust contracts; and
- learned feedback weights, personalized signals, and feedback-loop guardrails, which require held-out evaluation,
  feature governance, and deployment controls beyond the fixed transparent example policy.

Do not infer any deferred identity or probability from click aggregates. Add each as a focused capability with its own
input contract, semantics, evaluation evidence, and caller-owned operational boundary.

## Evidence Standard

Every capability must have typed schemas, deterministic ordering where rank is exposed, online/generated parity tests,
generated source and traceability registration, concise example documentation, and focused validation for boundary
conditions. Live PySpark evidence is required before claiming runtime parity; missing local PySpark is a
test-environment limitation, not proof of behavior.
