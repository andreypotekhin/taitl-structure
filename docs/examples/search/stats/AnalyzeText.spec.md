# Search Text Analysis


`AnalyzeText` publishes descriptive statistics for the extracted hierarchy and compares caller-selected document feature
relations. It supports understanding a corpus without changing retrieval semantics.


Outputs describe sentence, paragraph, section, and document counts, word counts, distinct-term counts, average word
length, and selected similarity or comparison facts. Statistics retain source identifiers and grain identity.

The boundary is descriptive. It does not train a ranker, alter scores, select a corpus, or infer document quality from
length or lexical frequency.

## Design

Statistics were kept separate from Indexing so aggregate lexical artifacts and descriptive reporting can evolve
independently. Driver-side collection and hidden statistical filters were rejected. Feature comparisons remain
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


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Role | Alternatives in choices above | Public analysis boundary | Keeps lineage explicit |
| Empty input | Throw; return zeros; explicit empty state | Explicit empty state | Zero is not always meaningful. |
| Comparison | One aggregate; per-grain outputs; opaque vector | Per-grain outputs | Text hierarchy remains visible. |

Failures should name target grain, statistic, denominator, and source snapshot. Evidence must cover empty values,
shared terms, multiple hierarchy levels, and the same Search result with analysis omitted.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
