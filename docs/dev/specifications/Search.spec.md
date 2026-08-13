# Search Example Application

## Status and Authority

This specification defines the user-visible and schema-level contract of the Search example under
`examples/search/`. It describes the implementation present in the source tree as of 2026-08-07 and records the
architecture forks resolved in earlier design and Codex discussions.

The broader rationale is summarized in this document through the evidence boundaries and architecture choices below.
The Search README is the concise usage guide, while the historical decision and execution-plan names in the final
section preserve the decisions that led to this contract. When an implementation detail changes, update this
specification first, then the usage documentation that depends on it.

## Purpose

Search turns a caller-owned document corpus and caller-owned serving evidence into typed, deterministic search
evidence. A caller can:

- split plain-text documents into sections, paragraphs, and sentences;
- build reusable lexical indexes and score queries at each text grain;
- retrieve sentences, passages, and documents with explicit rank boundaries;
- use attributed impressions and clicks as transparent reranking evidence;
- find same-grain corpus neighbours;
- evaluate judged relevance separately from observed served behavior; and
- slice ranking and evaluation by labels, user-band contexts, and named experiments.

Search is an example application of Structure, not a hosted search service. It returns DataFrame relations that a caller
may persist, serve, page, cite, compare, or pass to a separate answering system.

## Scope and Ownership

Structure owns the typed transformations, schemas, deterministic ranking rules, compiler-visible dataflow, generated
PySpark artifacts, diagnostics, and online/generated execution parity.

The caller owns:

- document harvesting, source validation, and corpus replacement;
- embedding or model-provider execution when a future vector branch is used;
- persistence of corpus, index, score, feedback, and evaluation artifacts;
- cache refresh and snapshot selection;
- query serving, request construction, ranking-version deployment, and propensity assignment;
- streaming sources and sinks, watermarks, triggers, checkpoints, output mode, and restart policy; and
- answer generation, prompt assembly, and downstream materialization.

Search has no hidden corpus cache, storage adapter, scheduler, crawler, model invocation, driver-side corpus collection,
or cross-document answer prompt.

## Terms and Identity

The four searchable grains are document, section, paragraph, and sentence. Each grain has independent identifiers,
term statistics, scores, and rank partitions. A score or rank from one grain must not be reused as another grain's
evidence.

`SearchQuery.id` is the request-local query key used to partition score and rank relations. `SearchQuery.content` is
free-form text; callers do not pre-tokenize it. Its normalized form is the feedback aggregation key, so equivalent
query text may share historical feedback while request-local ranks remain separate. `SearchQuery.requested_at` is an
immutable event-time value and must equal the matching `SearchRequest.requested_at`. `SearchQuery.queryset` is a
caller-defined query population name used for evaluation slicing.

`SearchRequest.id` identifies one serving attempt, including an attempt that returns no results. It carries the query
identity, optional user and experiment identity, ranking version, and request time. `Impression.id` identifies one
displayed document and `Click.id` identifies one action against an impression.

`user_band_id` identifies the exact reusable context assigned to a user. `band_id` identifies a persisted cohort band
used by feedback and evaluation. These are not substitutes for a request ID or a single demographic field.

`experiment_id = null` denotes production. A named experiment must be explicitly selected and must preserve its
identity through score, result, request, and evaluation relations.

## Architecture

The application is split into caller-owned boundaries and typed Structure stages:

    caller corpus and query inputs
        -> Chunking -> Indexing -> reusable lexical artifacts
        -> OfflineScoring / Filtering -> timestamped offline artifacts
        -> optional labels, cohorts, and feedback -> reusable evaluation/reranking artifacts
        -> request-time OnlineFiltering / OnlineScoring
        -> SearchSentences, SearchPassages, or SearchDocuments
        -> caller-owned serving, persistence, citation, or answer generation

`All` is the one-call offline composition for corpus, query, similarity, labels, scores, cohorts, and relevance
artifacts. It intentionally excludes event aggregation, presentation, evaluation, experiments, training, and feature
engineering. Callers compose those boundaries explicitly.

The application uses small typed compositions rather than one opaque search transform. The major boundaries are:

| Boundary | Implemented contract |
| --- | --- |
| `Chunking` | Heading/blank-line hierarchy; punctuation splitter is a replaceable UDF boundary. |
| `Indexing` | Normalizes text once and emits four-grain term/summary relations. |
| `Scoring` | Produces overlap/BM25 families and selected timestamped scores. |
| `Filtering` / `OnlineFiltering` | Produces or fills timestamped simple-overlap document-filter artifacts. |
| `SearchSentences` | Emits deterministic sentence matches ranked by score evidence. |
| `SearchPassages` | Emits ranked paragraphs with same-section neighboring context. |
| `SearchDocuments` | Filters, obtains, and feedback-reranks document candidates through explicit bounded stages. |
| `similarity/lexical` | Reuses lexical artifacts for same-grain directed scoring and reciprocal corpus candidates. |
| feedback transforms | Converts impression/click events into daily facts and batch relevance snapshots. |
| evaluation transforms | Measures judged document quality and observed served behavior as separate facets. |
| experiments/training | Adds explicit score variants and an optional offline, manually promoted model branch. |

## Input and Artifact Contracts

The primary public schemas are:

- `Document`, `Section`, `Paragraph`, and `Sentence` for caller-owned text and extracted hierarchy;
- `SearchQuery`, `SearchRequest`, `Impression`, and `Click` for query and serving events;
- `SimilarityPolicy` and `ScorePolicy` for candidate, freshness, timestamp, and grain-specific score weights;
- `DocumentTerm`, `SectionTerm`, `ParagraphTerm`, `SentenceTerm`, and their summaries for reusable indexes;
- `DocumentScore`, `SectionScore`, `ParagraphScore`, and `SentenceScore` for unified selected scores;
- `DocumentFilterScore` for persisted or online simple-overlap filter artifacts;
- `DocumentSearchCandidate` and `DocumentSearchResult` for document retrieval and presentation;
- `User`, `Band`, `UserBand`, `BandMembership`, `UserBandMembership`, and `BandFallback` for reusable contexts;
- `QueryDocumentSignals`, `DocumentPopularity`, and `RelevancePolicy` for feedback evidence;
- evaluation batch, judgment, label, intent, and behavior schemas for offline evaluation; and
- training feature, snapshot, ranker, and artifact schemas for the optional model branch.

All public outputs have explicit Structure schemas. Intermediate relations may be private lanes, but their keys and
grain must remain deterministic and compiler-visible. Persistence is not implied by a schema or transform output.

## Text and Lexical Pipeline

### Chunking

`Document.content` is plain text similar to Markdown. A line beginning with `#` starts a section and supplies its
heading. Blank lines separate paragraphs. A document without a heading uses an implicit `Document` section. The
pipeline preserves document, section, paragraph, and sentence identifiers and deterministic local ordinals.

The default `SentenceChunking` implementation splits on terminal punctuation through a declared Python UDF. It is a
replaceable starting point, not a source-faithful sentence segmenter. A caller requiring exact sentence text or spans
must run `DocumentChunking`, supply a span-aware `Paragraph`-to-`Sentence` transform, and pass those
boundaries plus the original documents to `Indexing`.

### Indexing and normalization

`Indexing` consumes sentences and privately creates normalized term rows. The public model does not persist token
occurrences. The normalization contract is shared by extraction and query scoring: lowercasing, whitespace and
punctuation normalization, and term extraction are applied consistently.

Each grain has its own term frequency, target frequency, target length, distinct-term count, target count, vocabulary,
and average target length. An empty corpus has a defined summary result and does not cause a driver-side collection.

### Lexical scoring

For grain `g`, query `q`, and target `x`, let `Q` be the distinct normalized query terms and `T(x)` the target terms.
The IDF-weighted overlap score is:

    idf_g(t) = log(1 + (N_g - df_g(t) + 0.5) / (df_g(t) + 0.5))
    overlap_g(q, x) = sum(idf_g(t) for t in Q ∩ T(x)) / sum(idf_g(t) for t in Q)

Missing vocabulary terms use `df_g(t) = 0`; a zero denominator produces `0`. BM25 uses `k1 = 1.2` and `b = 0.75`:

    bm25_g(q, x) = sum(
        idf_g(t) * tf_g(t, x) * (k1 + 1)
        / (tf_g(t, x) + k1 * (1 - b + b * length_g(x) / average_length_g))
    )

`SelectScores` restricts score construction to the selected `DocumentSearchTarget` relation, then normalizes BM25
inside that target-local rank scope for the grain and combines it with overlap using independent `ScorePolicy` weights.
The normalized scopes are target-scope/query for documents, target-scope/query/document for sections,
target-scope/query/document/section for paragraphs, and target-scope/query/document/section/paragraph for sentences.
The index summary still supplies corpus-level IDF and average-length statistics. A zero BM25 maximum normalizes to `0`.
Scores are lexical evidence, not calibrated relevance probabilities.

`OfflineScoring` precomputes the configured popular query population and every query observed during the preceding
seven days. `OnlineScoring` resolves missing or stale query groups at request time. A score is usable only when it is
not future-dated, is no older than `maximum_age_days`, and is not older than `effective_at`; the latter invalidates
artifacts when the score policy changes.

## Document Retrieval and Presentation

### Filtering and retrieval funnel

Document retrieval uses the following explicit funnel:

    Filtering (offline selected queries)
      -> DocumentFilterScore artifacts
    OnlineFiltering (missing or stale query groups)
      -> online DocumentFilterScore artifacts
    SelectFilterTargets
      -> at most 10,000 simple-overlap document targets per query
    OnlineScoring
      -> one request-valid target-scoped composite lexical score relation and one request-valid vector score relation
    RankVectors
      -> one bounded vector candidate lane from the merged vector scores
    RetrieveDocuments
      -> unranked lexical and ranked vector candidate lanes
    FuseDocumentCandidates
      -> lexical rank, document-level deduplication, RRF, and at most 1,000 fused candidates
    RerankDocuments
      -> feedback enrichment, final rank, and at most 100 results

The filter counts distinct normalized query terms shared with a document. Ties are ordered by document ID. It is an
early performance boundary, not the final relevance score. The final lexical candidate set is selected before feedback
is applied, so feedback cannot invent a document absent from the admitted candidate set. When the caller supplies a
validated vector lane, vector candidates are fused with lexical candidates before feedback and a vector-only candidate
may enter without a lexical overlap row. The final cap is applied only after feedback reranking, allowing a candidate
ranked 101 through 1,000 in the fused lane to move into the returned top 100.

For a candidate, feedback is:

    0.8 * query-document feedback + 0.2 * document-popularity feedback

The reranker combines normalized retrieval score and feedback using `RelevancePolicy.score_weight` and
`RelevancePolicy.feedback_weight`. Missing feedback contributes zero. Final ties are deterministic by document ID.

### Sentence presentation

`SearchSentences` returns matching sentences for one or more queries. Rank is one-based and ordered by descending BM25,
descending overlap, document ID, and sentence ID. Consumers page by emitted rank, never by physical DataFrame order.

### Passage presentation

`SearchPassages` ranks paragraphs and adds the document title and URL, section heading, and nullable immediately
preceding and following paragraph content. Context is limited to the same document section. Neighbor paragraphs do not
contribute retrieval terms or rank; adjacent matching paragraphs remain separate results. Top-K, overlap removal, and
answer-context assembly belong to the caller.

## Feedback Evidence

The serving system emits one `SearchRequest` for every attempt, one `Impression` per displayed document, and zero or
more `Click` events. The serving system must provide a calibrated `examination_propensity` in `(0, 1]`; Search does
not infer propensity from position.

`Impressions` and `Clicks` are streaming transformations with a seven-day watermark and ID deduplication. Impressions
remain in daily exposure output when unclicked. A click is attributed only when it references an impression and occurs
from the impression's display time through 24 hours later. Orphan, duplicate, late, and out-of-window clicks produce
no attributed click fact. Click attribution follows the impression's display window rather than the click's calendar
day.

`BuildRelevanceSignals` consumes persisted daily facts in batch. It retains raw impressions, click events,
clicked-impression counts, dwell, and long clicks for observability. CTR counts an impression at most once even when it
has repeated clicks. Ranking feedback uses capped dwell, recency decay, and self-normalized inverse propensity
weighting. The default policy is a 30-day half-life, 70/30 dwell-to-CTR signal blending, and a 20-impression minimum
for the CTR component; dwell remains eligible below that threshold.

Feedback is ranking evidence and observed engagement. It is not relevance truth and must not be used to claim judged
precision, recall, or corpus coverage.

## Similarity

Lexical similarity creates a query from each target's normalized vocabulary, scores it against the same-grain lexical
index, and reduces directed scores to bounded same-grain candidate relations. It returns up to 10 neighbors per source target,
preserves both directed BM25 values, their mean, and overlap, and excludes self-pairs. BM25 is directional and
corpus-dependent; its mean is an inspection value, not a probability. An optional maximum document-frequency ratio
prunes common terms at each grain. Similarity does not impose hidden title, source, language, or collection filters.

The lexical similarity relations remain the reusable baseline. `SearchSimilarity` and the paragraph
`SearchSimilarity` funnel under `search_similarity/paragraphs` accept provider-neutral ranked vector candidates and
combine the lanes with RRF. The bundled exact vector index is a reference producer;
caller-owned ANN services may emit the same candidate contract. Document search remains on its separate
lexical/feedback path.

## Cohorts, Labels, Experiments, and Training

### User-band contexts

Users may match several caller-defined cohort predicates. `ResolveCohortBands` creates a deterministic, priority-ordered
reusable `UserBand`, its memberships, and a fallback chain. It retains the most-specific matching memberships and
weakens the least-important band first through its declared parent before reaching global feedback. It does not blend
unrelated bands. Missing parents and cycles are configuration errors. Anonymous or unmatched users use the global
context.

The hierarchy resolver remains a narrow raw Spark boundary because the DSL has no recursive-relation operation. Ordinary
matching, feedback, ranking, and evaluation stay typed. This boundary is tracked for later migration when a typed
recursive contract exists.

### Query labels and evaluation slices

Caller labels are timestamped and merged into `SearchQuery.labels`. Intent catalogs and locale-specific regular
expression patterns can create deterministic labels; labels are evaluation slices, not ranking inputs. Different label
names are combined with AND; multiple requested values for one name are alternatives; an empty label selection keeps
all queries. A null evaluation queryset keeps all query sets.

### Experiments

Production rows have `experiment_id = null`. Named score experiments must be explicitly active and identically shaped.
They flow through score selection, presentation, request logging, and evaluation. A reranking experiment may replace the
rerank stage through transform composition, but there is no hidden second experiment identity.

### Optional training branch

`BuildTrainingData` produces candidate-scoped judged snapshots. `TrainingPipeline` trains and evaluates the built-in
rankers, recommends an artifact deterministically, and leaves promotion to the caller. `RankDocumentCandidates` applies
one manually promoted, contract-validated artifact. Calling `SearchDocuments` without this transform retains the exact
lexical-plus-feedback fallback. Training is offline and separate from serving orchestration.

## Evaluation Contract

Search deliberately provides two evaluation facets that share a UTC-aligned daily `EvaluationBatch` but do not
substitute for one another.

`EvaluateDocumentRanking` consumes one result run and caller-supplied `DocumentRelevanceJudgment` rows. Grades are 0
(not relevant), 1 (related), 2 (relevant), and 3 (ideal). Grades 2 and 3 are binary relevant for Precision, judged
Recall, Success, and reciprocal rank; all grades affect nDCG. Metrics are emitted at cutoffs 5, 10, and 15. A returned
unjudged document makes the affected metric unavailable rather than silently nonrelevant. Compare runs against the same
judgment pool and persist the run identifier outside the generic result schema.

`EvaluateDocSearchBehavior` consumes actual requests, impressions, and clicks. It preserves no-result requests and
reports request-level and daily ranking-version behavior, including result counts, click and long-click outcomes, first
click/long-click rank, reciprocal first-long-click rank, and propensity-adjusted exposure measures. A long click is at
least ten seconds of dwell. These are observed-satisfaction metrics and must not be named Precision, Recall, MRR, or
relevance quality.

## Execution and Compiler Boundary

Search transforms must compile without importing PySpark, starting Spark, Java, or a cluster. Runtime execution may use
PySpark. Online execution and generated-code execution consume the same checked semantic contract and must agree on
schemas, rows, ranks, freshness behavior, and diagnostics.

Generated Search artifacts are Structure-owned outputs. They must never be hand-edited;
regenerate them through the repository's generation workflow.

Ordinary Search logic belongs in typed step methods. Raw boundaries are permitted only for documented capability gaps,
including the default sentence supplier, query-term expansion where required by the compiler, global index summaries,
similarity self-alias reduction, first-qualified feedback fallback, and cohort hierarchy resolution. A raw hook must not
be used to conceal ordinary ranking or business logic.

`SearchDocuments` currently declares streaming lineage but is classified `batch_only`. Its ranking windows,
deduplication, score normalization, and join shapes do not yet have a bounded Structured Streaming state contract. A
future streaming implementation must produce one append-only final result set per query after finite event-time
completion, use bounded top-K state, require exact-schema stream unions and bounded joins, and leave sources,
watermarks, checkpoints, triggers, sinks, and snapshot refreshes with the caller.

## Architecture Bifurcations and Resolved Choices

The following forks are part of the specification because changing one changes public schemas, evidence semantics, or
operational ownership.

| Bifurcation | Alternative considered | Choice and consequence |
| --- | --- | --- |
| Product boundary | Hosted service or typed pipeline | Typed pipeline; no harvesting, serving, model, or answer work. |
| Text representation | Publish occurrences or persist text on every grain | Keep text in `Document`; publish boundaries and aggregate terms. |
| Sentence segmentation | Universal spans or replaceable UDF | Use a span-aware default UDF; callers replace it with another span-aware supplier. |
| Composition shape | Monolith or typed stages | Independent stages preserve traceability and extension points. |
| Runtime artifact | Online-only/generated-only/divergent | Online default; generated uses the same contract. |
| Offline scoring | Score all online or precompute a population | Popular plus seven-day offline; online fills gaps. |
| Candidate gate | Feedback promotion, lexical-only, or broad overlap | `10,000 -> 1,000 -> top 100`. |
| Overlap meaning | Bounded, tie-breaker, or reused IDF score | IDF-weighted per grain; combine with BM25 weights. |
| Feedback evidence | Clicks as labels, CTR only, or separate | Impression-backed feedback; not relevance truth. |
| Evaluation | Blended, judged-only, or behavior-only | Separate judged quality and observed behavior facets. |
| User context | Arbitrary, blended, or ordered fallback | Deterministic `UserBand` with priority-tail fallback. |
| Experiment identity | Corpus/inferred/explicit IDs | Explicit ID across score, result, request, evaluation. |
| Compiler escape hatch | Raw PySpark or DSL-only | Typed steps plus narrow documented raw boundaries. |
| Streaming readiness | Declaration/rejection/bounded proof | `batch_only` until bounded state/restart are proven. |
| Semantic retrieval | Blend, concatenate, or lexical-only | Separate lanes; future RRF; source remains lexical. |
| Vector implementation | Hosted ANN/exact/model execution | Caller embeddings and exact retrieval first. |

## Future Bifurcations

Model execution and hosted ANN operation remain outside Search; the typed exact reference and candidate-fusion behavior
are current similarity behavior. Search vectorization now provides a typed adapter boundary for model execution: the
offline and online facets reuse compatible embeddings, infer only gaps, and emit successful embeddings and inference
statuses for caller-owned persistence. `SearchDocuments` runs `OnlineVectorization` after filter-target selection so
uncached documents are limited to the bounded serving target set. `InferencePolicy` carries provider/model identity and
permits arbitrary vector dimensions; the bundled deterministic adapter uses dimension 100 for tests and development.

### Vector index and Reciprocal Rank Fusion

The adopted plan `P08052602.Search-vector-index-and-rrf.plan.md` provides caller-supplied, validated document and
paragraph embeddings with model ID, dimension, content revision, and experiment ID. It provides cosine similarity,
rejection of empty or zero-norm vectors, exact top-K retrieval, and a separate `SimilarityFusionPolicy` for candidate
windows and presentation. The follow-up plan `P08102602.Similarity-search-hybrid-and-ann-backends.plan.md` records the
provider-neutral candidate boundary.

Lexical and vector candidates remain separate ranked lanes for similarity and the opt-in document-search path.
Reciprocal Rank Fusion contributes
`1 / (rrf_k + rank)` for each available lane, with equal weights and `rrf_k = 60` by default. A candidate found in only
one lane remains eligible; a candidate found in both retains both ranks and component scores. Fusion occurs before
feedback reranking. Sections and sentences remain lexical-only in the first slice.

The exact backend is an example/reference implementation, not a hosted ANN service. Callers can substitute HNSW or
another ANN producer by emitting the same ranked candidate relation; model execution, persistence, refresh scheduling,
and production ANN operations remain outside Structure.

### Structured Streaming document search

The deferred plan `P08022605.SearchDocuments-structured-streaming.plan.md` retains the desired append-only contract but
does not authorize a streaming claim. The implementation must replace unbounded business-key
deduplication and analytic ranking with bounded event-time state, prove stream/stream joins and exact-schema unions,
pre-resolve static feedback fallback and popularity, and define one finalization point. A caller handoff must bind one
immutable snapshot across indexes, score caches, feedback, popularity, and policy.

## Diagnostics and Failure Semantics

Search must fail early for invalid fundamental inputs rather than silently produce plausible evidence. At minimum,
implementations must identify the transform, relation, key, policy, and remedy for:

- inconsistent query/request timestamps;
- unusable or stale score/filter artifacts;
- invalid or cyclic cohort hierarchies;
- invalid propensity or malformed feedback identity;
- mismatched experiment or context keys; and
- unsupported streaming or backend capability requirements.

Diagnostics use the repository registry and link to the most specific documentation. A missing judgment is represented
as unavailable quality evidence, not as a fabricated nonrelevant judgment. A missing feedback row contributes zero to
reranking and does not remove a lexical candidate.

## Acceptance Criteria and Evidence

The Search implementation is conforming when the following are true:

1. A fixture corpus can be chunked, indexed, scored, and presented at all four text grains with declared schemas.
2. Query normalization is consistent between index construction, scoring, filtering, and feedback aggregation.
3. Empty, missing-vocabulary, zero-maximum, duplicate-event, orphan-click, late-click, no-result, and stale-cache cases
   have deterministic behavior.
4. Document filtering, 1,000-candidate retrieval, feedback reranking, and final top-100 output are separate observable
   boundaries.
5. Passage context never crosses a section heading and does not alter retrieval rank.
6. Similarity preserves directed BM25 evidence, canonical pair identity, same-grain isolation, self-exclusion, and
   deterministic top-10 ranks.
7. User-band fallback, label selection, active experiment selection, and training promotion are explicit opt-in paths.
8. Judged quality and observed behavior remain separate and produce the documented metrics and nullability semantics.
9. Online and generated execution agree on supported fixtures, generated output is current, and compiler checks remain
   Spark-free.
10. `make build` passes; any environment-gated PySpark skips are recorded rather than presented as live runtime proof.

The primary current evidence is the Search integration suite at
`tests/integration/pyspark/search/test_search.py`, generated-output coverage at
`tests/golden/test_examples_generated_output.py`, the Search fixture set under `examples/fixtures/search/`, and the
repository-wide build. Future vector and streaming claims require additional focused evidence described in their plans.

## Related Records

The recorded design context consists of the Search design, the overlap filtering and composite scoring plan
(`P08062601.Search-overlap-filter-and-composite-scoring.plan.md`), the SearchDocuments Structured Streaming plan
(`P08022605.SearchDocuments-structured-streaming.plan.md`), the vector index and RRF plan
(`P08052602.Search-vector-index-and-rrf.plan.md`), the Search evaluation facets decision
(`D07222601.Search-evaluation-facets.md`), and the Search example README. Their roles are defined here: design sets
the capability boundary, plans record deferred implementation slices, and the decision records explain why observed
behavior and judged relevance remain separate.
