# Search Features


The features boundary prepares reusable document and query attributes for the optional ranking-training path. It is not
the serving ranker and does not claim that descriptive text statistics are relevance signals.


Document features are derived from caller-owned documents and indexed text. Query features are derived from query
identity, normalized content, labels, and declared query metadata. Feature relations retain stable keys so they can be
joined to candidate-scoped training snapshots.

Feature creation must be deterministic for a fixed corpus and query snapshot. It must not collect corpus data on the
driver, call a model provider, or mutate serving state.

## Design

Features remain separate from `All`'s default serving path so lexical-plus-feedback Search stays usable without model
artifacts. A hidden learned ranker was rejected; feature generation, training, promotion, and inference are explicit
boundaries. Personalized or embedding features remain future extensions with their own contracts.


Feature rows are schema-valid, key-compatible with candidate and judgment relations, reproducible from a fixed snapshot,
and optional: omitting them preserves the baseline Search path.


Mismatched snapshot identity, duplicate feature keys, incompatible data types, or missing required feature contracts
must fail before training or inference. Evidence should cover empty feature inputs, sparse query features, and repeated
training runs from the same snapshot.


| Concern | Contract |
|---|---|
| Document key | Features retain document, tenant/corpus, and source snapshot identity. |
| Query key | Query features retain request identity and cannot be joined by text alone. |
| Snapshot | Offline and online features name a compatible effective timestamp/version. |
| Joinability | A feature row has one declared grain; many-to-one reduction is explicit. |
| Values | Types, nullability, ranges, and missing-value policy are contractual. |
| Provider boundary | Custom providers return schema and provenance without hidden global state. |

Features are derived evidence, not an implicit cache. Recomputing one source snapshot produces the same keys and
values. A missing feature may trigger a documented default or make the branch ineligible; it must not silently
join to another document, query, tenant, or future snapshot.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Ownership | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Missing values | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Serving integration | Alternatives in choices above | Shared relation contract | Keeps lineage explicit |

Failure evidence must include feature key, expected type, source snapshot, and provider identity. Fixtures should
cover empty inputs, sparse query features, duplicate keys, incompatible types, and repeated computation.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
