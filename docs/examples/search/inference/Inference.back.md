# Search Inference

`Inference` is the provider boundary for turning Search queries and documents into embeddings. The caller selects a
concrete adapter and supplies an `InferencePolicy` carrying provider, model, revision, experiment, timestamp, and
dimension identity.

Query and document inference are separate typed operations. Each emits a successful embedding lane and a status lane,
so a provider failure remains observable while lexical Search can continue. Successful vectors are validated against
the policy before they become cacheable Search artifacts. The default adapter is deterministic and dependency-free;
production adapters remain caller-owned and replaceable.

## How it works

- Validate policy identity and positive dimension before invoking a provider.
- Invoke query and document adapters through separate compiler-visible transforms.
- Publish successful vectors with model, dimension, revision, and experiment identity.
- Publish one status row per attempted query or document, including failure code and diagnostic text.
- Preserve lexical fallback for failed query inference and remove only the failed document from the vector lane.

Online vectorization passes `streaming=True`; offline preparation passes `streaming=False`. The boundary performs no
network ownership, persistence, scheduling, or hidden caching.

| Boundary | Contract |
|---|---|
| Policy | Provider/model identity and positive embedding dimension are valid before inference. |
| Adapter | Query and document operations are replaceable and streaming-aware. |
| Embedding | Successful vectors match policy dimension and compatibility identity. |
| Status | Every attempt remains observable with success or failure details. |
| Ownership | Callers persist and refresh embeddings; the transform owns no mutable cache. |

Diagnostics should identify the provider, model, revision, experiment, target key, and declared dimension. Evidence
should cover deterministic defaults, arbitrary dimensions, malformed policy, invalid provider vectors, query failure,
document failure, and offline/online mode selection.
