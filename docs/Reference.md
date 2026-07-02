# Reference

This page collects specification documents that are useful as public reference material. The specifications are more
detailed than the quick guides and define the behavior Structure aims to keep stable.

## Core Authoring

- [DSL](specifications/DSL.md): public Python API for schemas, transforms, expressions, joins, hooks, and sessions.
- [Schema declaration syntax](specifications/SchemaDeclarationSyntax.md): `Structure`, `field(...)`, aliases, nested
  fields, and supported declaration forms.
- [Schema inheritance](specifications/SchemaInheritance.md): field reuse and ordered schema composition.
- [Schema semantics](specifications/SchemaSemantics.md): schema meaning across declarations, construction,
  validation, and assignment compatibility.
- [Nullability and type coercion](specifications/NullabilityAndTypeCoercion.md): assignment safety, nullable values,
  literals, widening, and explicit conversion helpers.

## Transforms And Execution

- [Online execution](specifications/OnlineExecution.md): running transforms with `StructureSession` and live
  DataFrames.
- [PySpark code generation](specifications/PySparkCodeGeneration.md): generated module layout, readable PySpark output,
  schema constants, and generated-file behavior.
- [Execution semantic contract](specifications/ExecutionSemanticContract.md): shared meaning between online execution
  and generated PySpark.
- [Symbolic execution](specifications/SymbolicExecution.md): how compiled subtransform methods become compiler-visible
  plans.
- [Intermediate representation](specifications/IntermediateRepresentation.md): backend-neutral transform plan model.

## Joins, Hooks, And Validation

- [Join semantics](specifications/JoinSemantics.md): lookup joins, join keys, cardinality, null behavior, and scoped
  field references.
- [Analytical join coverage](specifications/AnalyticalJoinCoverage.md): existence joins, row-multiplying joins,
  deterministic dedupe, and temporal lookup joins.
- [Hook semantics](specifications/HookSemantics.md): explicit runtime escape hatches and target-scoped hook behavior.
- [Validation semantics](specifications/ValidationSemantics.md): schema validation phases, modes, strictness, hooks,
  and output projection.
- [Data quality constraints](specifications/DataQualityConstraints.md): future value-level constraints and their cost
  boundary.

## Operations

- [Configuration schema](specifications/ConfigSchema.md): configuration keys, defaults, resolution, and diagnostics.
- [CLI](specifications/CLI.md): `structure init`, `check`, `compile`, `inspect`, `clean`, and schema tools.
- [Source module rules](specifications/SourceModuleRules.md): source roots, imports, discovery, and generated import
  mapping.
- [Diagnostics](specifications/Diagnostics.md): error and warning format, stability, severity, and documentation links.
- [Streaming compatibility](specifications/StreamingCompatibility.md): when transforms can be used with streaming
  DataFrames.
- [Compatibility policy](specifications/CompatibilityPolicy.md): Python, PySpark, backend, generated-code, and
  versioning compatibility.

## Extension Points

- [Backend capabilities](specifications/BackendCapabilities.md): feature support checks for configured execution
  targets.
- [Alternative backends](specifications/AlternativeBackends.md): future backend-extension boundary.
