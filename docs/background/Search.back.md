# Search Example Background

The Search example shows how to build transparent search evidence from a caller-owned document corpus. It is a
collection of typed transformations, not a hosted search product: callers supply documents and query batches, choose
where artifacts live, serve results, and decide how an application turns evidence into an answer.

The example deliberately keeps three kinds of evidence separate:

- lexical scores say how well normalized terms match corpus text;
- interaction signals say how users behaved after seeing a result list; and
- relevance judgments say how assessors rated retrieval quality.

None is a calibrated probability of relevance, and none is silently substituted for another.

The executable source is the [Search example](../../examples/search/Readme.md) and its transforms under
`examples/search/transforms`. The governing [Search design](../dev/design/Search.md) defines the evidence boundaries.
This background is synchronized to those example contracts; it does not introduce a separate search API or a
hosted-service promise.

## Corpus Ownership and Freshness

`Document.content` is plain text. Heading lines form sections, blank lines form paragraphs, sentences are derived
inside paragraphs, and words are normalized once for all lexical paths. The resulting hierarchy retains document,
section, paragraph, sentence, and word identifiers with deterministic ordinals.

The caller owns harvesting, source validation, persistence, corpus snapshots, and index refreshes. A search invocation
uses precisely the documents and index artifacts supplied to it; the example has no hidden corpus cache or freshness
schedule. This lets an application choose its own current-corpus policy, including a batch replacement, an incremental
pipeline, or a separately operated serving index.

`CreateIndex` creates independent document, section, paragraph, and sentence index artifacts. Term frequency,
document frequency, target length, target count, vocabulary size, and average target length are specific to the text
grain. A score computed for a document is therefore never reused as a paragraph or sentence score.

## Lexical Retrieval

Queries use the same normalization rules as extracted words. `SearchQuery.id` is the request-local key for scoring and
rank. Query text is also the normalized key used when feedback aggregates observations across equivalent searches.

`ScoreOverlap` provides a bounded distinct-term overlap measure. `ScoreBm25` provides BM25 with the example's fixed
parameters (`k1 = 1.2`, `b = 0.75`). They are separate outputs so that a caller can select or combine evidence
explicitly. BM25 is corpus-dependent; neither score is a relevance probability.

Unified score rows carry a `scored_at` timestamp. `ScorePolicy` defines the timestamp used when producing a snapshot and
the maximum age accepted by serving. Offline scoring aggregates daily impression volume by normalized query and caps
the popular population, then adds every query observed in the preceding seven days; arbitrary older production queries
are resolved online.

The presentation transforms expose deterministic ranks. Consumers should page by emitted rank rather than relying on
physical DataFrame order:

- `SearchSentences` ranks matching sentences by BM25, overlap, document ID, and sentence ID.
- `SearchPassages` ranks matching paragraphs by BM25, overlap, document ID, and paragraph ID.
- `SearchDocuments` retrieves a lexical candidate set before applying feedback-aware reranking.

Every query in one batch remains independently ranked. The deterministic identifier tie-breakers ensure equal lexical
scores do not produce a nondeterministic result order.

## Passage Search and Answer Evidence

Paragraphs are the first passage unit because extraction and indexing already preserve them independently and because
they commonly retain a useful authorial thought boundary. `SearchPassages` ranks the matching paragraph and supplies
at most one preceding and one following paragraph as display or answer context.

The context window stays inside the paragraph's document section. A heading boundary therefore yields a null preceding
or following value rather than context borrowed from another topic. Neighboring paragraphs never contribute terms or
scores to the matched paragraph's rank. If adjacent paragraphs both match, they remain distinct result rows; the caller
can later deduplicate or merge context for a particular answer experience.

Each passage result carries its document title and nullable source URL, as well as the section heading and matched and
neighboring content. Those fields make the row suitable as directly attributable evidence. The example does not invoke
an answer model, choose a top-K, create a cross-document prompt, or synthesize a final answer. Those choices belong to
the application because they depend on its model, citation policy, latency budget, and user experience.

## Document Retrieval and Feedback

Document search begins with `OnlineScoring`. It treats the caller's existing score relations as cache-compatible
inputs, filters stale or future rows, and calculates missing query groups from the reusable lexical index. It emits only
the bridge rows calculated for the current request. Retrieval unions those rows with the caller's pre-calculated stored
and streamed rows, so the caller can persist the bridge output in the same score relation and reuse it on a repeat
query.
No separate cache schema, query-key field, or index-version field is required.

The remaining three stages are `RetrieveDocuments`, `OverlapDocuments`, and `RerankDocuments`. Retrieval admits up to
1000 persisted or streamed candidates per query by descending score with a document-ID tie-breaker. Overlap narrows
those candidates to 100 by overlap score. Rerank enriches only those candidates with feedback and ranks their combined
score.
A document outside the lexical candidate set cannot enter only because it is popular or has historical clicks.

Within a candidate set, BM25 is normalized by the query's maximum candidate score. Caller-supplied relevance-policy
weights combine that normalized lexical score with feedback evidence. A document with no feedback remains eligible and
has zero feedback contribution. Overlap is an explicit narrowing boundary before feedback, not a final reranking
ingredient.

Feedback starts with immutable `SearchRequest`, `Impression`, and `Click` records. Every attempt produces a request,
including a no-result attempt. An impression records the displayed position and its logged examination propensity; a
click records the impression it follows and its dwell duration.

Streaming processing produces daily impression and attributed-click facts. A click must reference an impression and
fall between its display time and 24 hours afterward. Duplicate, late, orphaned, and out-of-window clicks do not
become attributed facts. Batch relevance aggregation then applies recency decay, inverse-propensity correction, capped
dwell credit, and independent normalization for query-document and document-wide evidence.

### What CTR means here

CTR is binary at the impression level: the numerator is the number of impressions with at least one attributed click,
and the denominator is the number of impressions shown. Raw `click_count` remains alongside it as an engagement
metric. For example, two clicks on one displayed document produce a click count of two but one clicked impression;
the result's CTR is still at most one.

This distinction avoids treating repeated clicks as extra exposures while retaining useful behavior evidence such as
reopening, repeated navigation, and accumulated dwell. Attributed clicks use the impression's display day rather than
the click's calendar day, so a click shortly after midnight remains attached to the result list that produced it.

### How interaction becomes a ranking signal

The serving system logs an examination propensity with every impression. The snapshot uses it in a self-normalized
IPS CTR ratio: weighted clicked impressions divided by weighted shown impressions, where both have the same recency
weight and inverse propensity. Lower propensity therefore gives a qualifying observed event more correction weight;
it does not manufacture a propensity model from rank alone.

Two safeguards keep the example's feedback deliberately conservative. First, IPS dwell is capped, log-scaled, and
normalized before it can influence rank. Second, CTR does not contribute until a query-document or document-wide
aggregate has at least 20 impressions. The default signal blend is 70% normalized dwell and 30% IPS CTR. Query-specific
and global signals are then combined 80/20 before the document reranker combines feedback with normalized BM25.

These are transparent example-policy defaults, not universal retrieval constants. Applications choose their own
half-life, score weights, and threshold, validate them against judgments and business objectives, and deploy them as a
versioned serving policy.

Logged propensity is a serving-system responsibility. Position alone is not a propensity model. Feedback is useful
ranking evidence, but it can reflect result position, interface design, traffic mix, and user intent, so it is never
treated as offline relevance truth.

## Similarity

Similarity reuses the lexical index rather than introducing an embedding model. It creates a query from each target's
vocabulary, scores targets at the same text grain, and reduces directed scores into bounded same-grain neighbors. The
result retains overlap, both directed BM25 scores, and their mean for inspection. The mean is a convenience value, not
a probability.

Callers may prune common terms through a maximum document-frequency ratio and may apply source, language, access, or
collection restrictions appropriate to their product. Such constraints are application policy, not hidden similarity
behavior.

## Evaluation

Search quality has two complementary, non-interchangeable facets.

`EvaluateDocumentRankingQuality` measures a document ranking against caller-provided four-grade relevance judgments: 0
(not
relevant), 1 (related), 2 (relevant), and 3 (ideal). Grades 2 and 3 count as binary relevant, while all grades affect
nDCG. Per-query and daily outputs include Precision, judged Recall, Success, nDCG, and reciprocal-rank measures at
the supported cutoffs. An unjudged returned document makes the affected judgment-based metric unavailable rather than
silently counting as nonrelevant.

`EvaluateDocumentSearchBehavior` measures the list actually served. It retains no-result requests, attributes clicks
to displayed impressions, and reports request-level behavior and daily summaries by ranking version. Outputs include
result and click counts, first click and long-click ranks, and exposure-adjusted long-click and dwell-credit rates. A
long click has dwell time of at least ten seconds.

Behavior measures observed satisfaction with the served experience. It must not be called Precision, Recall, MRR, or
relevance quality, which require an explicit relevance-judgment contract. Compare ranking runs using the same persisted
judgment pool; monitor a deployed version through its own request and impression facts.

## Boundaries and Deferred Work

The Search example is intentionally not a crawler, a document store, an answer service, a prompt assembler, an
embedding or vector-search implementation, or a streaming-job framework. It also does not prescribe adaptive passage
chunking, configurable context radii, experiment comparison, counterfactual policy evaluation, or session
reformulation metrics.

It also defers propensity calibration and clipping, bot and accidental-click filtering, user or session
personalization, learned feedback weights, and feedback-loop controls. Those capabilities need additional identity,
experiment, trust, or governance contracts. They cannot be recovered reliably from anonymous daily click aggregates.

Those additions need focused input contracts and clear evidence semantics. Keeping them outside the base example
preserves a small, inspectable foundation for lexical retrieval, contextual passages, feedback-aware ranking, and
evaluation.
