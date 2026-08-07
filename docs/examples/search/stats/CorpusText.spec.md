# Search Corpus Text


`CorpusText` summarizes corpus-wide document statistics and vocabulary facts from already-derived relations.


The boundary publishes document count, average hierarchy sizes, word and distinct-word summaries, word-length
distribution facts, and an independently estimated corpus vocabulary. Shared terms are counted once in the corpus
vocabulary; per-document vocabularies must not simply be summed.

`CorpusText` consumes aggregate inputs and does not tokenize raw text, persist artifacts, or decide query eligibility.

## Design

Corpus summaries were separated from lexical scoring to keep statistics reusable and to make empty-corpus behavior
explicit. Summing document vocabularies was rejected because shared terms would be double-counted. Approximate sketches
remain a design choice only where their determinism and error contract is explicit.


Corpus-wide facts are reproducible, vocabulary semantics are distinct from per-target vocabulary, and summary output is
not required for ordinary query serving.


| Output | Contract |
|---|---|
| Corpus length | One snapshot-aligned total with declared normalization and denominator. |
| Vocabulary | Corpus/term identity, document frequency, and configured count semantics. |
| Target vocabulary | Per-document or per-passage terms remain distinct from corpus vocabulary. |
| Summary | Optional aggregate relation; not a prerequisite for ordinary serving. |

Corpus facts derive from a fixed membership relation. Adding a document changes the corpus snapshot and may change
global statistics, but must not mutate historical artifacts in place. A term absent from the vocabulary has
explicit semantics; it is not confused with a term whose count is zero.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Vocabulary | Alternatives in choices above | Both with distinct keys | Keeps lineage explicit |
| Input | Alternatives in choices above | Bounded snapshot | Keeps lineage explicit |
| Use | Alternatives in choices above | Optional artifact | Keeps lineage explicit |

Failure evidence must include corpus identity, membership snapshot, term, and denominator. Fixtures should compare
empty, one-document, repeated-term, and shared-term corpora.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
