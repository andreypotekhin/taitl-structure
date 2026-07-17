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

The example deliberately remains batch-only: corpus distributions and
near-duplicate comparisons need a bounded input corpus. The latter uses title
prefix, source, and language as a candidate block before calculating
Levenshtein distances, avoiding an unrestricted self-Cartesian join.

`corpus.corpus_statistics` reports document-level averages and the distribution
of document-average word length. `corpus.corpus_vocabulary` independently
estimates the distinct corpus vocabulary from word rows; it never sums
per-document vocabularies, which would double-count shared words.
