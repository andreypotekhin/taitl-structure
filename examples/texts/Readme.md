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

`ScoreCorpus` combines the independently usable `ScoreOverlap` and `ScoreBm25` transforms. It accepts caller-supplied
`SearchQuery(id, content)` rows and creates a score row for every matching document, section, paragraph, and sentence.
The algorithms normalize query terms exactly as
`ExtractText` normalizes document words. `score_overlap` is the standard overlap coefficient: matching distinct terms
divided by the smaller of the query and target vocabularies. `score_bm25` uses fixed `k1=1.2` and `b=0.75` constants.
The scores remain separate: choosing an algorithm or combining parent and child targets is deliberately caller-owned.

Search logic uses Spark SQL functions inside a narrow raw boundary for query-token row expansion. A Python UDF would
serialize every scored row through Python and hide its logic from Spark's optimizer, preventing whole-stage code
generation and limiting projection and predicate optimization. A vectorized Pandas UDF reduces that overhead but
remains an opaque boundary and needs Arrow batches. Use a UDF only for a tokenizer or scoring rule that cannot be
expressed through Spark's native relational functions.

```python
scores = ScoreCorpus(queries=queries, words=segments.words).run(session)
overlap = scores.document_overlap_scores
bm25 = scores.document_bm25_scores
```

The example deliberately remains batch-only: corpus distributions and
near-duplicate comparisons need a bounded input corpus. The latter uses title
prefix, source, and language as a candidate block before calculating
Levenshtein distances, avoiding an unrestricted self-Cartesian join.

`corpus.corpus_statistics` reports document-level averages and the distribution
of document-average word length. `corpus.corpus_vocabulary` independently
estimates the distinct corpus vocabulary from word rows; it never sums
per-document vocabularies, which would double-count shared words.
