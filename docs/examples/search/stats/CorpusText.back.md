# Search Corpus Text


`CorpusText` summarizes corpus-wide document statistics and vocabulary facts from already-derived relations.


The boundary publishes document count, average hierarchy sizes, word and distinct-word summaries, word-length
distribution facts, and an independently estimated corpus vocabulary. Shared terms are counted once in the corpus
vocabulary; per-document vocabularies must not simply be summed.

`CorpusText` consumes aggregate inputs and does not tokenize raw text, persist artifacts, or decide query eligibility.

## How it works

Corpus summaries were separated from lexical scoring to keep statistics reusable and to make empty-corpus behavior
explicit. Corpus vocabulary counts shared terms once across the membership snapshot; approximate sketches remain a
design choice only where their determinism and error contract is explicit.


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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Vocabulary | Summed; mixed key; distinct keys | Both with distinct keys | Keys keep statistics distinct. |
| Input | Live state; unbounded history; snapshot | Bounded snapshot | Snapshots preserve history. |
| Use | Mandatory serving; hidden cache; optional artifact | Optional artifact | Serving does not require summaries. |


Failures should identify corpus identity, membership snapshot, term, and denominator. Examples should compare
empty, one-document, repeated-term, and shared-term corpora.
