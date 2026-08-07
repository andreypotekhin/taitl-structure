# Search Experiments


The experiments boundary demonstrates how to compare named ranking variants without putting experiment state on corpus
documents or hiding variant selection inside a scorer.


Production uses a null experiment identity. Named scores and reranking variants carry explicit IDs through score
selection, presentation, request logging, and evaluation. An experiment must be active, shape-compatible, and evaluated
against the same query population, labels, user context, batch, and judgment pool as its baseline.

Experiment composition may replace a scoring or reranking boundary while preserving the surrounding Search contract.
There is no implicit second experiment identity for reranking.

## Design

Corpus-level experiment fields were rejected because corpus facts should be reusable across runs. Inferring the variant
from result rows was rejected because it weakens input validation and lineage. Named, explicit, nullable identity was
chosen for traceability and deterministic comparison.


Production and named rows remain distinguishable, inactive or unknown experiments fail early, and evaluators never blend
variant rows into one comparison.


Diagnostics should distinguish unknown experiment, inactive experiment, duplicate variant rows, and mismatched policy or
snapshot identity. Evidence must show production, named, and absent-variant cases with stable result lineage.


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


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| State location | Alternatives in choices above | Row-level snapshot identity | Keeps lineage explicit |
| Variant composition | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Evaluation | Alternatives in choices above | Descriptive comparison | Keeps lineage explicit |

Diagnostics should distinguish unknown, inactive, duplicate, and incompatible experiment state. Evidence must
show stable assignment under reordered input and clear lineage for baseline, named, and absent-variant cases.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
