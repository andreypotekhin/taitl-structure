# Search Training


Training is an optional offline branch for testing whether caller-supplied features and judgments can support a
versioned ranking artifact. It does not silently replace lexical Search.


`BuildTrainingData` creates a candidate-scoped, judged snapshot. `Training` and `TrainingPipeline` split by complete
query
identity, train supported transparent rankers, evaluate them against the lexical baseline, and choose a recommendation
deterministically. The caller manually promotes exactly one validated artifact.

`RankDocumentCandidates` applies one promoted artifact only when the caller explicitly composes that branch. Without it,
`SearchDocuments` remains lexical-plus-feedback.

## Design

Training and serving were separated to prevent unreviewed model promotion and snapshot leakage. The example uses simple
rankers rather than a provider-specific model runtime. Learned weights, embeddings, online training, and automatic
promotion require separate feature governance and deployment contracts.


Train/validation splits keep whole queries together, artifacts carry compatible feature and version identity, malformed
artifacts fail before ranking, and the baseline remains a complete fallback.


| Stage | Contract |
|---|---|
| Dataset | Rows retain query, candidate, label, feature, tenant/corpus, and snapshot identity. |
| Split | Whole queries stay together; split policy and seed are recorded. |
| Artifact | Model/policy carries feature schema, source snapshot, version, and evaluation metadata. |
| Validation | Type, range, key, and compatibility checks run before publication. |
| Serving | Missing or incompatible artifacts select the declared baseline. |

Training is an offline artifact boundary. It must not read future feedback relative to the split or leak the same
query across train and validation. The model can be replaceable, but its input/output contract and provenance
are not optional. Publishing an artifact does not activate it for Search.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Lifecycle | Alternatives in choices above | Train/validate/publish | Keeps lineage explicit |
| Split | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Baseline | Alternatives in choices above | Lexical fallback | Keeps lineage explicit |

Failure evidence must name artifact, feature schema, split policy, source snapshot, and validation error. Fixtures
should include query leakage, incompatible features, malformed artifacts, empty training data, and fallback serving.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
