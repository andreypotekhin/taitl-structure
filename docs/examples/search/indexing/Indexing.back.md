# Search Indexing


`Indexing` turns document-backed sentence boundaries and extracted document fields into reusable Search artifacts for
query batches and similarity runs. It materializes source text only in private transient lanes before tokenization.


The index publishes independent lexical term and summary relations for documents, sections, paragraphs, and sentences,
plus positional field-term postings for metadata search. Lexical term rows contain target-local frequency and length
facts plus grain-level frequency; field rows retain field identity and token positions. Token occurrences remain private
intermediate data.

The same normalization contract is used for sentence extraction, free-form query text, filtering, and scoring. The
caller may persist any aggregate relation and must treat all of them as belonging to one corpus snapshot.

## How it works

Indexing is separate from chunking so source segmentation and lexical normalization can evolve independently. Word
occurrences remain private to this boundary, avoiding an unnecessary public API and persistence burden. Statistics are
grain-specific because document, passage, and sentence relevance have different distributions.


All grains are isolated, empty-corpus behavior is defined, term and summary keys are stable, and the same input snapshot
produces identical artifacts in online and generated execution.


Duplicate target identity, inconsistent parent IDs, negative or impossible lengths, summary/term snapshot mismatch, and
unsupported empty-corpus behavior must fail or be explicitly represented. Evidence should compare all four grains,
shared terms across targets, punctuation normalization, and an empty corpus.


| Grain | Identity | Required facts |
|---|---|---|
| Document | Tenant/corpus/document | Length, normalized text, and source snapshot. |
| Section | Document/section | Parent identity, ordinal, and source span. |
| Paragraph | Section/paragraph | Parent identity, ordinal, and source span. |
| Sentence | Paragraph/sentence | Parent identity, ordinal, and source span. |

Index facts are immutable for a source snapshot. Normalization must be shared by term counts, vocabulary, and
candidate lookup; otherwise a query and its target can disagree about what a term means. Shared terms may be
represented once in a corpus vocabulary while target rows retain their own frequencies.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Normalization | Per-consumer; chunking; indexing boundary | Declared boundary | Search stages agree. |
| Statistics | Global; hidden recompute; corpus artifact | Corpus artifact | Reuse stays explicit. |
| Public relation | Publish tokens; mixed relation; per-grain | Relations per grain | Consumers get one grain. |

Field indexing is part of this composition as the `Indexing.fields` child. It remains a separate implementation lane
inside the transform so it does not alter `LexIndex` artifacts or add body-content positions.


Failures should identify target identity, parent identity, normalization policy, and snapshot. Examples should compare
every grain, shared terms, punctuation variants, and an empty corpus.
