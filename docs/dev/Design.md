# Design

This is the entry point for maintained design documentation. Topic documents in
[`design/`](design/) explain durable architecture and contracts; version-specific documents from V4 through V9 are
historical pointers or source material for the current register below. Specifications define implementation-ready
behavior, while this page records the current design boundary and the gates that remain open.

The register reflects the V10 closeout review on 2026-08-22. A gate is not a generic backlog item: it identifies a
missing contract or proof required before Structure can make a stronger support claim.

## Design Map

- Architecture and target boundary: [Background](design/Background.design.md),
  [Plugin Architecture](design/PluginArchitecture.design.md), [Architecture](Architecture.md), and
  [Plugin Authoring](PluginAuthoring.md).
- API admission and coverage: [API Catalog Design Gates](design/ApiCatalogDesignGates.design.md),
  [API Catalog](../APICatalog.md), and [API Gaps](Gaps.md).
- Schemas and typed relations: [Schema Model](design/SchemaModel.design.md) and
  [Typed Relation Operations](design/TypedRelationOperations.design.md), with the
  [Typed Relation Operations specification](specifications/TypedRelationOperations.spec.md).
- Expressions and compilation: [DSL](design/DSL.design.md),
  [Intermediate Representation](design/IntermediateRepresentation.design.md), and
  [Compileability Checker](design/CompileabilityChecker.design.md).
- Online and generated execution: [Execution Semantic Contract](design/ExecutionSemanticContract.design.md) and
  [PySpark Code Generator](design/PySparkCodeGenerator.design.md).
- Streaming: [Spark Streaming](design/SparkStreaming.design.md) and
  [Deferred Streaming Features](design/SparkStreamingDeferredFeatures.design.md), with the
  [Streaming API reference](../api/Streaming.api.md).
- Diagnostics and evidence: [Diagnostics Contract](design/DiagnosticsContract.design.md),
  [Discovery and Inspection](design/DiscoveryInspection.design.md), and
  [V10 Release Evidence](project-management/V10ReleaseEvidence.md).
- Search and analytical domains: [Search](design/Search.design.md) and
  [Advanced Analytical Operations](design/AdvancedAnalyticalOperations.design.md).

## Design Gates

These are the outstanding contract gates after V10. The status names match the API catalog. `design-gated` means a
contract is described but Structure does not yet support the API. `streaming-ineligible` means the shape requires a
batch boundary. `caller-owned-guided` means Structure provides a runnable boundary recipe without owning the API.

### Streaming missing-column union — `design-gated`

Streaming schema evolution needs explicit cardinality, nullability, nested-field, alias, and state semantics, plus
PySpark 3.5/4.0 evidence. Use exact-schema streaming unions or materialize to batch. See
[API Catalog gates](design/ApiCatalogDesignGates.design.md) and the
[V10 API plan](planning/P08022601.V10-api-catalog-and-schema-evolution.plan.md).

### Variant mutation profiles — `design-gated`

Append, insert, set, and delete helpers require a released target profile, typed paths and results, capability rules,
and online/generated evidence. Use the released Variant slice; keep mutation helpers target-gated until the profile is
released. See [API Catalog gates](design/ApiCatalogDesignGates.design.md).

### Logical join reordering — `design-gated`

Reordering must be opt-in, dependency-safe, explainable, hint-preserving, and conservative around hooks, assertions,
dedupe, projections, and streaming joins. Joins retain source order; no public `join_order(...)` helper exists. See
[API Catalog gates](design/ApiCatalogDesignGates.design.md) and the
[join decision](past/decisions/D07302602.Join-reordering-deferred.md).

### XML helpers — `design-gated`

A schema-carrying parser/serializer contract still needs output schema, options, nullability, malformed-record behavior,
and target support. Keep XML caller-owned and low priority. See
[API Catalog gates](design/ApiCatalogDesignGates.design.md).

### Broader chained stateful streaming — `design-gated`

Beyond the admitted chained-window shape, each state stage needs event-time and watermark sources, retention, output
mode, composition rules, diagnostics, generated form, and restart evidence. Keep the one-stateful-plus-stateless policy;
use caller-owned PySpark for other chains. See [Spark Streaming](design/SparkStreaming.design.md) and the
[V10 streaming plan](planning/P08022602.V10-streaming-state-and-join-contracts.plan.md).

### Row-level `foreach` — `design-gated`

Any Structure support would need sink identity, idempotence, retry, security, checkpoint, and recovery contracts. Keep
`foreach` outside transforms and generated modules. `foreachBatch` remains `caller-owned-guided`. See
[Spark Streaming](design/SparkStreaming.design.md).

### Arbitrary state processors — `design-gated`

`applyInPandasWithState` and `transformWithState` need typed state, input, and output schemas; timeout and clock policy;
lifecycle hooks; target profiles; generated boundaries; and restart evidence. `ArbitraryStateContract` validates
adoption metadata only; callers own the state runtime. See the
[V10 side-effect and state plan](planning/P08022603.V10-streaming-side-effects-and-arbitrary-state.plan.md).

### SearchDocuments streaming proving lane — `design-gated`

The batch graph still needs bounded ranking state, finite stream/stream joins, append-final output,
snapshot immutability, restart recovery, and a caller-owned lifecycle handoff. Keep SearchDocuments batch-focused until
the proving lane
produces positive evidence. See [V10 Release Evidence](project-management/V10ReleaseEvidence.md).

## Evidence Gates

These items do not change the design boundary, but they prevent a support claim until the environment or target lane
provides positive evidence:

- PySpark 3.5/4.0 live streaming and Spark Connect lanes remain unavailable when the Docker engine cannot be reached.
- Optional Geometry provider evidence is target-gated and is not bundled with the PySpark plugin.
- `is_valid_variant(...)` has capability evidence for its released profile but no positive PySpark 4.2 live lane in this
  workspace.
- Exact Search vector retrieval and the Search generated/online comparison still need a live target lane.

Unavailable evidence is recorded as unavailable, never promoted to `implemented` or `supported`. The current matrix is
maintained in [V10 Release Evidence](project-management/V10ReleaseEvidence.md).

## Versioned Design History

The V4–V9 files below remain as compatibility pointers for existing links. Their active conclusions are represented by
the maintained topic documents and the Design Gates register above:

- V4: [caller-owned streaming migration](design/V4CallerOwnedStreamingMigration.design.md) and
  [transformation API coverage](design/V4TransformationApiCoverage.design.md).
- V6: [PySpark API closure](design/V6PySparkApiClosure.design.md).
- V7: [caller-owned streaming adoption](design/V7CallerOwnedStreamingAdoption.design.md),
  [deferred PySpark families](design/V7DeferredPySparkFamilies.design.md), and
  [generator expansion](design/V7PySparkGeneratorExpansion.design.md).
- V9: [API catalog gates](design/V9ApiCatalogDesignGates.design.md) and
  [streaming gates](design/V9StreamingDesignGates.design.md).

Do not add new V-numbered design files for follow-up work. Add durable contract text to the appropriate topic design,
specification, or execution plan, then update the gate register when its status changes.
