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

## Design

- Publish sections, paragraphs, and sentences, but not a public word relation. Repeated tokenization by every consumer
  was rejected; shared normalization belongs in Indexing.
- Preserve explicit ordinals and parent IDs rather than relying on DataFrame order.
- Keep exact sentence spans caller-replaceable. The default splitter supplies spans, while custom splitters must also
  provide spans; content-only replacement rows are rejected.
- Keep adaptive chunk sizes and context-radius policies deferred; they belong to a separate passage design.


The caller supplies source text and chooses whether the resulting hierarchy is persisted. Chunking performs no storage,
answer assembly, or language-model work. Acceptance requires stable identifiers, section-local paragraph order,
replaceable sentence input, and online/generated parity for the supported fixture.


Invalid parent IDs, duplicate identifiers, impossible ordinals, and incompatible replacement schemas must fail before
downstream indexing. Evidence should cover heading-less documents, adjacent blank lines, multiple sections, punctuation
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


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Public grain | Alternatives in choices above | Explicit hierarchy | Keeps identity and evidence explicit |
| Sentence source | Alternatives in choices above | Replacement seam | Keeps the default replaceable |
| Ordering | Alternatives in choices above | Parent-local ordinal | Keeps execution deterministic |
| Context | Alternatives in choices above | Same-parent context | Prevents boundary crossing |

Failure evidence must distinguish rejected text from a lost parent, and must report the document and parent keys.
Fixtures should compare default and replacement suppliers on schema, parentage, and determinism rather than on
implementation-specific token boundaries.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
