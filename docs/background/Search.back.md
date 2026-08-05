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

## Search Pipeline At A Glance

The example is easiest to understand as four separately inspectable flows:

```text
document text
  -> hierarchy extraction
  -> normalized lexical index
  -> grain-specific scoring
  -> ranked document, passage, or sentence results

served result list
  -> impression and click facts
  -> attributed daily facts
  -> batch relevance signals
  -> feedback-aware reranking

target document and corpus index
  -> same-grain similarity query
  -> directed scores
  -> bounded similarity pair

served ranking + judgments
  -> behavior metrics or judgment metrics
```

Each arrow is a typed transformation boundary. A later flow consumes the persisted relation produced by an earlier flow;
it does not reach backward into an opaque cache or recompute an unrelated grain's score.

## Corpus Ownership and Freshness

`Document.content` is plain text. Heading lines form sections, blank lines form paragraphs, sentences are derived
inside paragraphs, and words are normalized once for all lexical paths. The resulting hierarchy retains document,
section, paragraph, sentence, and word identifiers with deterministic ordinals.

The caller owns harvesting, source validation, persistence, corpus snapshots, and index refreshes. A search invocation
uses precisely the documents and index artifacts supplied to it; the example has no hidden corpus cache or freshness
schedule. This lets an application choose its own current-corpus policy, including a batch replacement, an incremental
pipeline, or a separately operated serving index.

### Deterministic Text Extraction

The extraction boundary preserves the hierarchy needed by every later scoring grain:

```python
class ExtractDocumentText(Transform):
    documents = input(Document)
    sections = output(DocumentSection)
    paragraphs = output(DocumentParagraph)
    sentences = output(DocumentSentence)

    def extract(self, document: Document) -> tuple[
        DocumentSection,
        DocumentParagraph,
        DocumentSentence,
    ]:
        return extract_text_hierarchy(
            document,
            section_separator="heading",
            paragraph_separator="blank_line",
            sentence_separator="sentence",
        )
```

The helper name above represents the example's typed extraction stages; callers should use the concrete transforms in
`examples/search/transforms/chunking` rather than treating it as a generic text parser. Each output carries source
identity and an ordinal so later context and ranking remain reproducible.

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

### Grain-Specific Scoring

The scorer consumes the index at the same text grain as the requested result:

```python
class ScoreDocuments(Transform):
    queries = input(SearchQuery)
    document_terms = input(DocumentIndexTerm)
    scores = output(DocumentScore)

    def score(self, query: SearchQuery, term: DocumentIndexTerm) -> DocumentScore:
        inner_join(
            term,
            on=(term.normalized_term == query.normalized_term),
        )
        return DocumentScore(
            query_id=query.id,
            document_id=term.document_id,
            overlap_score=score_overlap(query, term),
            bm25_score=score_bm25(query, term, k1=1.2, b=0.75),
        )
```

The concrete example expands query terms and joins term statistics before calculating overlap and BM25. The conceptual
snippet emphasizes the boundary: document scores are not reused for paragraph or sentence results. Each score carries
the query, target, text grain, and scoring timestamp needed by retrieval and serving freshness policy.

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

### Passage Context Example

```python
class SearchPassages(Transform):
    queries = input(SearchQuery)
    paragraphs = input(DocumentParagraph)
    paragraph_scores = input(ParagraphScore)
    passages = output(SearchPassage)

    def present(
        self, query: SearchQuery, paragraph: DocumentParagraph, score: ParagraphScore
    ) -> SearchPassage:
        inner_join(
            score,
            on=(score.query_id == query.id)
            & (score.paragraph_id == paragraph.id),
        )
        return SearchPassage(
            query_id=query.id,
            paragraph_id=paragraph.id,
            title=paragraph.title,
            section_heading=paragraph.section_heading,
            content=paragraph.content,
            score=score.bm25_score,
            rank=score.rank,
        )
```

Neighbor context is selected within the same document section and is nullable at a heading boundary. It is display
evidence, not additional scoring input. A caller can later apply a product-specific top-K or context budget without
changing lexical ranking semantics.

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

### Retrieval And Reranking Boundaries

```python
class SearchDocuments(Transform):
    queries = input(SearchQuery)
    lexical_scores = input(DocumentScore)
    feedback = input(DocumentRelevanceSignal)
    results = output(RecommendedDocument)

    def retrieve(self, query: SearchQuery, score: DocumentScore) -> CandidateDocument:
        where(score.query_id == query.id)
        order_by(score.bm25_score, score.document_id)
        limit(1000)
        return CandidateDocument(
            query_id=query.id,
            document_id=score.document_id,
            lexical_score=score.bm25_score,
        )

    def rerank(
        self, candidate: CandidateDocument, feedback: DocumentRelevanceSignal
    ) -> RecommendedDocument:
        left_join(feedback, on=feedback.document_id == candidate.document_id)
        return RecommendedDocument(
            query_id=candidate.query_id,
            document_id=candidate.document_id,
            final_score=combine_scores(
                candidate.lexical_score,
                coalesce(feedback.feedback_score, 0),
            ),
        )
```

The concrete Search pipeline uses separate retrieval, overlap, and rerank stages. The example illustrates the invariant:
feedback enriches an admitted lexical candidate set; it does not create new candidates outside that set.

Within a candidate set, BM25 is normalized by the query's maximum candidate score. Caller-supplied relevance-policy
weights combine that normalized lexical score with feedback evidence. A document with no feedback remains eligible and
has zero feedback contribution. Overlap is an explicit narrowing boundary before feedback, not a final reranking
ingredient.

Feedback starts with immutable `SearchRequest`, `Impression`, and `Click` records. Every attempt produces a request,
including a no-result attempt. An impression records the displayed position and its logged examination propensity; a
click records the impression it follows and its dwell duration.

### Impression And Click Attribution

```python
@transform(streaming=True)
class AttributeClicks(Transform):
    impressions = input(Impression, streaming=True)
    clicks = input(Click, streaming=True)
    attributed = output(AttributedClick)

    def attribute(
        self, impression: Impression, click: Click
    ) -> AttributedClick:
        watermark(impression.displayed_at, delay="7 days")
        watermark(click.clicked_at, delay="7 days")
        inner_join(
            click,
            on=(click.impression_id == impression.id)
            & event_time_between(
                impression.displayed_at,
                click.clicked_at,
                upper="24 hours",
            ),
        )
        return AttributedClick(
            impression_id=impression.id,
            click_id=click.id,
            display_date=to_date(impression.displayed_at),
            dwell_seconds=click.dwell_seconds,
        )
```

The stream is responsible for bounded attribution facts. Late, orphaned, duplicate, or out-of-window clicks do not
become facts merely because they exist in a source relation. Batch aggregation can then apply the policy for recency,
propensity correction, and dwell credit.

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

### Feedback Signal Contract

```python
def build_query_document_signal(
    impression: DailyImpression,
    click: DailyAttributedClick,
    policy: RelevancePolicy,
) -> QueryDocumentSignal:
    group_by(
        query_text=impression.normalized_query,
        document_id=impression.document_id,
    )
    return QueryDocumentSignal(
        query_text=impression.normalized_query,
        document_id=impression.document_id,
        impression_count=count(),
        clicked_impression_count=count_distinct(
            click.impression_id,
            where=click.impression_id.is_not_null(),
        ),
        ips_ctr=compute_ips_ctr(impression, click, policy),
        normalized_dwell=compute_capped_dwell(click, policy),
    )
```

This is policy-bearing evidence, not a relevance label. The output should retain counts and thresholds so a caller can
inspect why a signal was eligible, zeroed, or omitted.

## Similarity

Similarity reuses the lexical index rather than introducing an embedding model. It creates a query from each target's
vocabulary, scores targets at the same text grain, and reduces directed scores into bounded same-grain neighbors. The
result retains overlap, both directed BM25 scores, and their mean for inspection. The mean is a convenience value, not
a probability.

### Same-Grain Similarity

```python
class SimilarParagraphs(Transform):
    query = input(SimilarityParagraphQuery)
    paragraphs = input(Paragraph)
    scores = input(ParagraphSimilarityScore)
    similar = output(SimilarParagraph)

    def publish(
        self,
        query: SimilarityParagraphQuery,
        paragraph: Paragraph,
        score: ParagraphSimilarityScore,
    ) -> SimilarParagraph:
        inner_join(
            score,
            on=(score.query_id == query.id)
            & (score.target_id == paragraph.id),
        )
        return SimilarParagraph(
            source_id=query.source_id,
            target_id=paragraph.id,
            forward_bm25=score.forward_bm25,
            reverse_bm25=score.reverse_bm25,
            mean_score=(score.forward_bm25 + score.reverse_bm25) / 2,
        )
```

The target and query must use the same text grain. Similarity does not imply semantic equivalence, an embedding score,
or a calibrated probability. It is a bounded, inspectable lexical relationship.

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

### Evaluation Input Choice

```python
def evaluate_quality(
    self, result: RecommendedDocument, judgment: RelevanceJudgment
) -> DocumentRankingMetric:
    left_join(
        judgment,
        on=(judgment.query_id == result.query_id)
        & (judgment.document_id == result.document_id),
    )
    return DocumentRankingMetric(
        query_id=result.query_id,
        rank=result.rank,
        judged_grade=judgment.grade,
        judged=judgment.grade.is_not_null(),
    )
```

An unjudged result is not automatically nonrelevant. The metric stage must preserve enough information to mark a
judgment-based metric unavailable when its required evidence is missing. Behavior evaluation uses impressions and
clicks instead; it must not be labeled Precision, Recall, MRR, or relevance quality.

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
