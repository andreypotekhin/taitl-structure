# Search Indexing


`Indexing` turns chunked sentences into reusable lexical artifacts for all Search query batches and similarity runs.


The index publishes independent term and summary relations for documents, sections, paragraphs, and sentences. Term
rows contain target-local frequency and length facts plus grain-level frequency; summaries contain target count and
average length. Token occurrences remain private intermediate data.

The same normalization contract is used for sentence extraction, free-form query text, filtering, and scoring. The
caller may persist any aggregate relation and must treat all of them as belonging to one corpus snapshot.

## Design

Indexing is separate from chunking so source segmentation and lexical normalization can evolve independently. A public
word relation was rejected as an unnecessary API surface and persistence burden. A single shared statistic across all
grains was rejected because document, passage, and sentence relevance have different distributions.


All grains are isolated, empty-corpus behavior is defined, term and summary keys are stable, and the same input snapshot
produces identical artifacts in online and generated execution.


Duplicate target identity, inconsistent parent IDs, negative or impossible lengths, summary/term snapshot mismatch, and
unsupported empty-corpus behavior must fail or be explicitly represented. Evidence should compare all four grains,
shared terms across targets, punctuation normalization, and an empty corpus.


| Grain | Identity | Required facts |
|---|---|---|
| Document | Tenant/corpus/document | Length, normalized text, and source snapshot. |
| Section | Document/section | Parent identity, ordinal, and section text facts. |
| Paragraph | Section/paragraph | Parent identity, ordinal, and token statistics. |
| Sentence | Paragraph/sentence | Parent identity, ordinal, and searchable terms. |

Index facts are immutable for a source snapshot. Normalization must be shared by term counts, vocabulary, and
candidate lookup; otherwise a query and its target can disagree about what a term means. Shared terms may be
represented once in a corpus vocabulary while target rows retain their own frequencies.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Normalization | Alternatives in choices above | Declared boundary | Search stages agree. |
| Statistics | Alternatives in choices above | Explicit corpus artifact | Keeps lineage explicit |
| Public relation | Alternatives in choices above | Relations per grain | Keeps lineage explicit |

Failure evidence must include target identity, parent identity, normalization policy, and snapshot. Fixtures
should compare all grains, shared terms, punctuation variants, and an empty corpus.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
