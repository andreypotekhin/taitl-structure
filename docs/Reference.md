# Reference

This page collects public reference material. These documents are more detailed than the quick guides and define the
behavior Structure aims to keep stable.

For function-by-function support, PySpark parity, examples, and discrepancies, start with
[API Reference](APIRef.md).

## Core Authoring

- [DSL](reference/DSL.md): public Python API for schemas, transforms, expressions, joins, hooks, and sessions.
- [Schema declaration syntax](reference/SchemaDeclarationSyntax.md): `Structure`, `field(...)`, aliases, nested
  fields, and supported declaration forms.
- [Schema inheritance](reference/SchemaInheritance.md): field reuse and ordered schema composition.
- [Schema semantics](reference/SchemaSemantics.md): schema meaning across declarations, construction,
  validation, and assignment compatibility.
- [Nullability and type coercion](reference/NullabilityAndTypeCoercion.md): assignment safety, nullable values,
  literals, widening, and explicit conversion helpers.

## Transforms And Execution

- [Online execution](reference/OnlineExecution.md): running transforms with `StructureSession` and live
  DataFrames.
- [PySpark code generation](reference/PySparkCodeGeneration.md): generated module layout, readable PySpark output,
  schema constants, and generated-file behavior.
- [Execution semantic contract](reference/ExecutionSemanticContract.md): shared meaning between online execution
  and generated PySpark.
- [Symbolic execution](reference/SymbolicExecution.md): how compiled step methods become compiler-visible
  plans.
- [Transform inheritance and composition](reference/TransformComposition.md): reusable parent transform fragments,
  `.to(...)` pipelines, wrapper pipelines, and composition limits.
- [Intermediate representation](reference/IntermediateRepresentation.md): backend-neutral transform plan model.

## Joins, Hooks, And Validation

- [Join semantics](reference/JoinSemantics.md): lookup joins, join keys, cardinality, null behavior, and scoped
  field references.
- [Analytical join coverage](reference/AnalyticalJoinCoverage.md): existence joins, row-multiplying joins,
  deterministic dedupe, and temporal lookup joins.
- [Full PySpark join support](reference/FullPySparkJoinSupport.md): right, full, cross, non-equi, and disjunctive
  rowset joins, plus explicit Cartesian acknowledgement and rowset projection rules.
- [Advanced analytical operations](reference/AdvancedAnalyticalOperations.md): broader aggregation, window, and
  higher-order collection helpers, including rollups, cubes, filtered metrics, reusable windows, and symbolic HOFs.
- [Hook semantics](reference/HookSemantics.md): explicit runtime escape hatches and target-scoped hook behavior.
- [Validation semantics](reference/ValidationSemantics.md): schema validation phases, modes, strictness, hooks,
  and output projection.
- [Data quality constraints](reference/DataQualityConstraints.md): future value-level constraints and their cost
  boundary.

## Operations

- [Configuration schema](reference/ConfigSchema.md): configuration keys, defaults, resolution, and diagnostics.
- [Generated documentation](reference/GeneratedDocs.md): generated Markdown and JSON schema/transform reference
  artifacts.
- [Testing helpers](reference/TestingHelpers.md): reusable pytest helpers for compiler checks, generated snapshots,
  diagnostics, and online/generated parity.
- [CLI](reference/CLI.md): `structure init`, `check`, `compile`, `inspect`, `clean`, and schema tools.
- [Source module rules](reference/SourceModuleRules.md): source roots, imports, discovery, and generated import
  mapping.
- [Diagnostics](reference/Diagnostics.md): error and warning format, stability, severity, and documentation links.
- [Streaming compatibility](reference/StreamingCompatibility.md): when transforms can be used with streaming
  DataFrames.
- [Spark streaming](reference/SparkStreaming.md): caller-owned Structured Streaming first-slice support.
- [Spark streaming deferred features](reference/SparkStreamingDeferredFeatures.md): generated streaming source, sink,
  lifecycle, watermark, and state-policy work left outside the first slice.
- [Spark Connect](reference/SparkConnect.md): PySpark Connect target variant, supported batch scope, diagnostics, and
  exclusions.
- [Compatibility policy](reference/CompatibilityPolicy.md): Python, PySpark, backend, generated-code, and
  versioning compatibility.

## Extension Points

- [Backend capabilities](reference/BackendCapabilities.md): feature support checks for configured execution
  targets.
- [Alternative backends](reference/AlternativeBackends.md): future backend-extension boundary.
