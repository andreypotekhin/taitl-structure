# Texts Example

This batch-only example models a harvested document corpus as typed Structure
schemas, turns caller-extracted text into sections, paragraphs, sentences, and
words, then computes local and corpus-wide analytics.

`Document.content` is plain text, not HTML. A line beginning with `#` starts a
section and supplies its heading; blank lines separate paragraphs. Documents
without a heading use an implicit `Document` section. `ExtractText` uses a
small `@raw` PySpark hook for `posexplode`, because Structure deliberately
defers row-generating functions until it has a first-class cardinality contract.
Every profile, aggregate, window, and similarity operation after extraction is
ordinary typed Structure.

```python
segments = ExtractText(documents=documents).run(session)
features = ProfileDocuments(documents=documents).run(session).features
analytics = AnalyzeText(
    words=segments.words,
    paragraphs=segments.paragraphs,
    sections=segments.sections,
    comparison_left=features,
    comparison_right=features,
).run(session)
corpus = CorpusText(documents=analytics.document_statistics, words=segments.words).run(session)
```

`CreateIndex` builds reusable document, section, paragraph, and sentence index artifacts from the extracted words. Each
term row holds its target-local frequency and vocabulary/length facts plus the token's target-grain document frequency;
each summary holds target count and average target length. Persisting those batch artifacts is caller-owned.

`AddScores` combines the independently usable `ScoreOverlap` and `ScoreBm25` transforms. It accepts caller-supplied
`SearchQuery(id, content)` rows plus matching index artifacts and creates a score row for every matching document,
section, paragraph, and sentence.
The algorithms normalize query terms exactly as
`ExtractText` normalizes document words. `score_overlap` is the standard overlap coefficient: matching distinct terms
divided by the smaller of the query and target vocabularies. `score_bm25` uses fixed `k1=1.2` and `b=0.75` constants.
The scores remain separate: choosing an algorithm or combining parent and child targets is deliberately caller-owned.

`CreateSimilarityQueries` and `ReduceSimilarityScores` reuse those same artifacts for corpus self-similarity. The first
turns each document, section, paragraph, and sentence vocabulary into a tagged query; pass the combined query rows to
`ScoreAll`, then give its directed score rows to the reducer. The reducer emits each same-grain pair once, with the
bounded symmetric overlap coefficient, both BM25 directions, and their arithmetic mean. BM25 remains directional and
corpus-dependent, so `bm25_mean` is a convenience for ranking rather than a calibrated similarity probability.

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
```

Search logic uses Spark SQL functions inside a narrow raw boundary for query-token row expansion. A Python UDF would
serialize every scored row through Python and hide its logic from Spark's optimizer, preventing whole-stage code
generation and limiting projection and predicate optimization. A vectorized Pandas UDF reduces that overhead but
remains an opaque boundary and needs Arrow batches. Use a UDF only for a tokenizer or scoring rule that cannot be
expressed through Spark's native relational functions.

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

`Search` is the final, query-scoped presentation boundary. It accepts one caller-supplied `SearchQuery` row and the
pre-scored sentence output from `AddScores`, then returns every matching sentence as a `SentenceSearchResult`.
`rank` is a one-based, deterministic ordering by BM25, overlap, document ID, and sentence ID; Spark DataFrames do not
promise physical row order, so consumers sort or page by `rank`. The one-query input is a caller contract: `Search`
remains lazy and does not count query rows.

```python
result = Search(
    query=query,
    scored_sentences=scores.scored_sentences,
).run(session)
ranked_sentences = result.results
```

The example deliberately remains batch-only: corpus distributions and
near-duplicate comparisons need a bounded input corpus. The latter uses title
prefix, source, and language as a candidate block before calculating
Levenshtein distances, avoiding an unrestricted self-Cartesian join.

`corpus.corpus_statistics` reports document-level averages and the distribution
of document-average word length. `corpus.corpus_vocabulary` independently
estimates the distinct corpus vocabulary from word rows; it never sums
per-document vocabularies, which would double-count shared words.
