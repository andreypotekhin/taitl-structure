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

## Design

Stable assignment was chosen for reproducibility and tenant isolation. Inferred exposure was rejected because an
assigned
variant may not have been served. Descriptive comparison was chosen because causal evaluation needs randomized selection
probabilities, treatment ownership, and guardrail definitions not present in the current contract.


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


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Assignment key | Alternatives in choices above | Stable eligible key | Keeps lineage explicit |
| Exposure | Alternatives in choices above | Served event | Keeps lineage explicit |
| Evaluation | Alternatives in choices above | Descriptive comparison | Keeps lineage explicit |

Failures must distinguish unknown/inactive experiment, duplicate variant, missing eligibility, and absent served
event. Evidence should cover repeated assignment, inactive variants, assigned-not-served rows, and reordered input.


The corresponding implementation boundary is named by this document under `examples/store/transforms/`.
Its typed input/output definitions live under `examples/store/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
