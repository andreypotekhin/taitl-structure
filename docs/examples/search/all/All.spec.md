# Search Artifact Build


`All` is the app-facing offline composition for preparing a Search snapshot. It gives a caller one typed boundary for
building corpus structure, lexical artifacts, similarity relations, query labels, offline scores, cohort contexts, and
feedback signals before request-time presentation.


`All` accepts caller-owned documents, queries, policies, label catalogs, persisted daily feedback facts, users, and
bands. It emits extracted hierarchy, statistics, lexical term and summary relations, similarity pairs, labeled queries,
score families, selected scores, resolved contexts, and relevance snapshots.

The output is a set of independent relations, not one materialized search database. The caller decides which relations
to persist, how to version a snapshot, and when to replace it. A one-row policy is required where a policy relation is
used.


The composition follows this order:

    corpus text -> chunking -> indexing -> scoring and similarity
    query catalogs -> labeling -> offline query selection and scoring
    users and bands -> context resolution -> context-aware feedback
    daily facts -> relevance snapshot

Presentation, event aggregation, evaluation, experiments, and training are deliberately outside `All`. This keeps the
pre-serving build bounded and makes runtime request behavior independently testable.

## Design

- Use one composition for reusable preparation, while keeping each major stage independently callable.
- Keep storage, scheduling, and snapshot replacement caller-owned.
- Build practical offline query coverage rather than attempting to score every historical query.
- Make labels, cohorts, and feedback inputs explicit so a caller can omit optional behavior without changing lexical
  semantics.


A monolithic service was rejected because it would hide persistence and serving decisions. An all-online design was
rejected because corpus statistics, similarity, decayed feedback, and broad query coverage require bounded snapshots.
An implicit cache was rejected because snapshot identity and freshness must remain visible to the caller.

Vector artifacts are not accepted by the current `All` contract. The adopted future vector plan requires a separate,
explicit policy and provider-owned embedding relations before extending this composition.


The boundary is conforming when the same fixture inputs produce stable output schemas and keys, optional labels or
feedback do not alter unrelated lexical outputs, and the caller can persist each artifact independently. Generated and
online runs must expose the same output set and semantics.


The build must fail for invalid one-row policies, inconsistent schema identity, invalid cohort hierarchies, or malformed
feedback keys. A successful build proves relation shape and deterministic composition; it does not prove that a caller's
storage writes, refresh schedule, or serving snapshot handoff is correct.


| Concern | Contract |
|---|---|
| Input snapshot | One coherent corpus, query, policy, feedback, and optional experiment snapshot. |
| Output grain | The declared target relation; orchestration must not silently union incompatible grains. |
| Identity | Tenant, corpus, query, document, and experiment keys remain present where they are meaningful. |
| Freshness | Every derived relation inherits or names the source snapshot and effective policy timestamp. |
| Optionality | Alternatives in choices above |
| Side effects | Alternatives in choices above |

The composition is a deterministic DAG. A downstream stage may consume a relation only when its identity,
grain, and snapshot metadata match the declared contract. This prevents a convenient join from turning a
document-level fact into an accidental sentence-level fact or from mixing online and offline states.


The compact decision record below makes the alternatives and selected boundary explicit.

| Decision point | Alternatives | Chosen result | Rationale |
|---|---|---|---|
| Preparation shape | Alternatives in choices above | Declared boundary | Keeps lineage explicit |
| Query coverage | Alternatives in choices above | Bounded funnel | Keeps lineage explicit |
| Artifact ownership | Alternatives in choices above | Caller-owned snapshots | Keeps lineage explicit |
| Semantic extension | Alternatives in choices above | Policy/provider boundary | Keeps lineage explicit |

Failure diagnostics must identify the first boundary that violated identity, grain, schema, or snapshot
compatibility. Evidence should include a complete baseline fixture, each optional branch independently absent,
and a deliberately incompatible artifact so that graceful fallback is distinguishable from silent omission.


The corresponding implementation boundary is named by this document under `examples/search/transforms/`.
Its typed input/output definitions live under `examples/search/schemas/`. The transform describes composition and
lifecycle; the schemas define identity, grain, nullability, and output keys. Those source paths orient an implementation
reader, but the contract above is intentionally consumable without opening them.
