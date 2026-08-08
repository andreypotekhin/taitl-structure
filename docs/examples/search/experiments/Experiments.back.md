# Search Experiments


The experiments boundary demonstrates how to compare named ranking variants without putting experiment state on corpus
documents or hiding variant selection inside a scorer.


Production uses a null experiment identity. Named scores and reranking variants carry explicit IDs through score
selection, presentation, request logging, and evaluation. An experiment must be active, shape-compatible, and evaluated
against the same query population, labels, user context, batch, and judgment pool as its baseline.

Experiment composition may replace a scoring or reranking boundary while preserving the surrounding Search contract.
There is no implicit second experiment identity for reranking.

## How it works

Corpus facts remain reusable across runs, so experiment identity lives on the evaluated row rather than on the corpus.
The variant is supplied explicitly instead of inferred from result rows; named, nullable identity preserves traceability
and deterministic comparison.


Production and named rows remain distinguishable, inactive or unknown experiments fail early, and evaluators never blend
variant rows into one comparison.



Diagnostics should distinguish unknown experiment, inactive experiment, duplicate variant rows, and mismatched policy or
snapshot identity. Examples should show production, named, and absent-variant cases with stable result lineage.


| Concern | Contract |
|---|---|
| Production | Baseline is a valid named state, not null or an accidental first row. |
| Variant | A query is evaluated against one active, uniquely identified variant. |
| Propagation | Experiment, variant, policy, and snapshot keys travel with scores and results. |
| Scope | Assignment scope is explicit: request, user, session, or another declared key. |
| Comparison | Variants share compatible input and output contracts before evaluation. |
| Promotion | Selecting a variant changes policy identity, not source evidence. |

Assignment is deterministic for a fixed eligible key and experiment salt. Emitting two result sets does not
claim causal impact; exposure and evaluation remain separate boundaries. An absent experiment selects baseline
behavior rather than manufacturing a random variant.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| State location | Corpus rows; inferred result state; row ID | Row snapshot | Lineage stays with evidence. |
| Variant composition | Hidden experiment; score ID; named boundary | Named boundary | Validate before comparison. |
| Evaluation | Causal claim; pooled variants; descriptive comparison | Descriptive | Causal claims need exposure. |

Diagnostics should distinguish unknown, inactive, duplicate, and incompatible experiment state. Examples should show
stable assignment under reordered input and clear lineage for baseline, named, and absent-variant cases.
