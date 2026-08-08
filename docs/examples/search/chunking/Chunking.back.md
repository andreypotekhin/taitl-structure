# Search Chunking


`Chunking` converts caller-provided plain-text documents into a stable hierarchy of sections, paragraphs, and
sentences. It creates the text grain used by indexing and presentation without deciding how documents are harvested or
how results are ultimately displayed.


A heading line beginning with `#` starts a section and supplies its heading. Blank-line groups form paragraphs. A
document without a heading receives an implicit document section. Outputs retain document identity, parent identity,
deterministic ordinals, and half-open Unicode code-point spans into the original document. Structural rows do not
persist duplicate content. Empty or malformed source content must have an explicit, documented result rather than an
invented document structure.

`Chunking` is the composed boundary. `DocumentChunking` establishes the document/section/paragraph hierarchy and
`SentenceChunking` supplies sentence boundaries for each paragraph; consumers materialize text privately from
`Document.content` when needed.


Chunking is intentionally upstream of lexical normalization. It owns structure and source order; `Indexing` owns term
normalization and aggregate lexical facts. This permits callers to replace sentence segmentation while preserving the
same downstream sentence schema.

The default sentence supplier is punctuation-based and declared as an explicit UDF boundary. It is useful for the
example but is not a universal language-aware segmenter.

The composition is intentionally small:

```python
chunked = DocumentChunking(documents=documents)
sentences = SentenceChunking(documents=documents, paragraphs=chunked.paragraphs)
```

The important behavior is the relation contract around these calls: parent keys and local ordinals survive the
transition, while a caller may replace sentence segmentation without changing downstream identity.

## How it works

- Publish sections, paragraphs, and sentences, but not a public word relation. Shared normalization belongs in Indexing,
  so every consumer does not need to repeat tokenization.
- Preserve explicit ordinals and parent IDs rather than relying on DataFrame order.
- Keep exact sentence spans caller-replaceable. The default splitter supplies spans, while custom splitters must also
  provide spans; content-only replacement rows are invalid because they cannot preserve source identity.
- Keep adaptive chunk sizes and context-radius policies deferred; they belong to a separate passage design.


The caller supplies source text and chooses whether the resulting hierarchy is persisted. Chunking performs no storage,
answer assembly, or language-model work. A conforming implementation preserves stable identifiers, section-local
paragraph order, replaceable sentence input, and online/generated parity for the supported fixture.


Invalid parent IDs, duplicate identifiers, impossible ordinals, and incompatible replacement schemas must fail before
downstream indexing. Useful examples cover heading-less documents, adjacent blank lines, multiple sections, punctuation
edge cases, and replacement-supplier parity.


| Boundary | Contract |
|---|---|
| Section | A section identity, ordinal, body span, and optional heading span. |
| Paragraph | A section child with stable ordinal and a document-local body span. |
| Sentence | A paragraph child with stable ordinal and a document-local sentence span. |
| Parentage | Every emitted child carries document and immediate-parent keys; no cross-document parent is valid. |
| Replacement | A supplied splitter may change boundaries but must emit the same schema and identity rules. |

Ordinals are local to their parent and are the ordering contract for downstream indexing and presentation.
Physical row order is not a substitute for an ordinal. Empty input, missing headings, and terminal punctuation
must have explicit outcomes so an empty relation is not confused with a malformed relation.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Public grain | Words too; documents only; hierarchy | Explicit hierarchy | Keeps identity and evidence explicit |
| Sentence source | Fixed splitter; opaque parser; replaceable | Replacement seam | Default can be replaced. |
| Ordering | Physical order; global ordinal; parent ordinal | Parent-local ordinal | Keeps execution deterministic |
| Context | Cross-section; whole-document; same-parent context | Same-parent context | Prevents boundary crossing |


Failures should distinguish rejected text from a lost parent and must report the document and parent keys.
Examples should compare default and replacement suppliers on schema, parentage, and determinism rather than on
implementation-specific token boundaries.
