# Search Text Analysis


`AnalyzeText` publishes descriptive statistics for the extracted hierarchy and compares caller-selected document feature
relations. It supports understanding a corpus without changing retrieval semantics.


Outputs describe sentence, paragraph, section, and document counts, word counts, distinct-term counts, average word
length, and selected similarity or comparison facts. Statistics retain source identifiers and grain identity.

The boundary is descriptive. It does not train a ranker, alter scores, select a corpus, or infer document quality from
length or lexical frequency.

## How it works

Statistics remain separate from Indexing so lexical artifacts and descriptive reporting can evolve independently.
Analysis does not collect data on the driver or introduce hidden statistical filters; feature comparisons remain
caller-selected rather than becoming implicit relevance policy.


Statistics are deterministic for a fixed hierarchy, empty values have explicit semantics, and analysis outputs do not
change Search ranking when omitted.


| Output family | Grain | Meaning |
|---|---|---|
| Length | Document/section/paragraph/sentence | Normalized text size under the shared policy. |
| Terms | Target/term | Count and distinctness for the target grain. |
| Distribution | Corpus or target | Aggregate statistics with explicit denominator. |
| Summary | Declared target | Optional presentation facts, not hidden ranking state. |

Aggregates carry the hierarchy and snapshot used to compute them. Empty input is a valid state with declared
null/zero semantics; it is not silently replaced with a global statistic. Adding or removing analysis output
does not change Search ranking unless a caller explicitly wires that output into scoring.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Role | Implicit ranking; driver report; analysis boundary | Analysis boundary | Analysis stays separate. |
| Empty input | Throw; return zeros; explicit empty state | Explicit empty state | Zero is not always meaningful. |
| Comparison | One aggregate; per-grain outputs; opaque vector | Per-grain outputs | Text hierarchy remains visible. |


Failures should name target grain, statistic, denominator, and source snapshot. Examples should cover empty values,
shared terms, multiple hierarchy levels, and the same Search result with analysis omitted.
