# Search Training


Training is an optional offline branch for testing whether caller-supplied features and judgments can support a
versioned ranking artifact. It does not silently replace lexical Search.


`BuildTrainingData` creates a candidate-scoped, judged snapshot. `Training` and `TrainingPipeline` split by complete
query
identity, train supported transparent rankers, evaluate them against the lexical baseline, and choose a recommendation
deterministically. The caller manually promotes exactly one validated artifact.

`RankDocumentCandidates` applies one promoted artifact only when the caller explicitly composes that branch. Without it,
`SearchDocuments` remains lexical-plus-feedback.

## How it works

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


The decisions below keep this topic inspectable when an implementation or provider changes.

| Decision | Alternatives considered | Choice | Why |
|---|---|---|---|
| Lifecycle | Auto-promotion; train-only; train/validate/publish | Train/validate/publish | Validate before publish. |
| Split | Random rows; time rows; query split | Query split | Split policy prevents leakage. |
| Baseline | No fallback; silent model; lexical fallback | Lexical fallback | Lexical fallback remains. |


Failures should identify artifact, feature schema, split policy, source snapshot, and validation error. Examples should
include query leakage, incompatible features, malformed artifacts, empty training data, and fallback serving.
