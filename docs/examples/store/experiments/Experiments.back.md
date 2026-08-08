# Store Recommendation Experiments


Store experiments provide stable tenant-scoped variant assignment, served exposure records, and descriptive comparison
of recommendation behavior.


Assignments use a stable hash of an eligible tenant-scoped customer, session, or request key and an active experiment
definition. Exposures are recorded only for actually served recommendation runs. Evaluation joins assignments to the
observed request, impression, click, and purchase facts for the selected variant.

Experiment identity and strategy/policy version remain explicit in outputs. Assignment does not itself serve a result,
change a product, or claim causal impact.

Assignment is a stable hash over the eligible tenant-scoped key and experiment identity. In compact form:

```text
variant = active_variants[hash(tenant_id, subject_key, experiment_id) mod variant_count]
```

The hash provides repeatability, not randomization proof. Served exposure remains a separate fact. Comparison is
observational until selection probabilities and guardrails are supplied; otherwise variant differences can describe
who was exposed rather than what the variant caused.

## How it works

Assignment is stable for reproducibility and tenant isolation. Exposure is recorded as a served event because an
assigned variant may not have been shown. Comparison remains descriptive because causal evaluation needs randomized
selection probabilities, treatment ownership, and guardrail definitions not present in the current contract.


The same eligible key maps consistently within an experiment, inactive variants are not selected, only served runs
create
exposures, and comparison output names its observational limitation.


| Concern | Contract |
|---|---|
| Eligibility | Assignment runs only for the declared tenant/request/customer population. |
| Assignment | One stable eligible key maps to one active variant within an experiment snapshot. |
| Exposure | An exposure is emitted only when the variant was actually served. |
| Identity | Experiment, variant, request, subject, policy, and snapshot keys propagate to results. |
| Evaluation | Comparisons name population and observational limitations. |

Assignment is deterministic and does not select inactive variants. An assigned-but-not-served subject is not an
exposure. The transform does not infer treatment adherence, causal effect, or rollout success from a comparison
alone; those require probabilities, guardrails, and ownership outside this boundary.


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Assignment key | Random; request; tenant key | Stable eligible key | Reordered input keeps assignment stable. |
| Exposure | Assignment row; served event; inferred exposure | Served event | Only exposure proves what was shown. |
| Evaluation | Causal; pooled; descriptive | Descriptive comparison | Exposure facts precede causal claims. |

Failures must distinguish unknown/inactive experiment, duplicate variant, missing eligibility, and absent served
event. Examples should cover repeated assignment, inactive variants, assigned-not-served rows, and reordered input.
