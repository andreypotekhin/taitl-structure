# Implementation

## Phase 1: v1 First Executable Slice

- Schema declarations.
- `input(...)`.
- `@transform` discovery.
- Single schema-returning step method.
- Symbolic field refs.
- One `@special(type="expr")` helper.
- `where(...)` filtering.
- Projection generation.
- Backend capability interface for first-slice PySpark requirements.
- Shared PySpark execution recipes for execution/generated-code parity.
- `StructureSession`.
- Builder-style transform invocation.
- Online PySpark execution of the first-slice transform.
- Spark `StructType` generation.
- Generated schema constants usable by caller code.
- Online-materialized output schema available from the transform invocation after `.run(session)`.
- Generated transform class.
- Input validation.
- Execution/generated-code parity test.
- CLI `check` for the first v1 fixture.

## Phase 2: v1 Complete

- Output validation.
- CLI `compile`.
- TOML config loading with explicit precedence and schema validation diagnostics.
- Compatibility policy enforcement for Python and target PySpark configuration.
- Backend capability checks for every supported v1 operation.
- Online PySpark runner for all v1 transform operations.
- Runtime target registry for execution and generated-code execution.
- Shared PySpark semantic lowering for every supported v1 operation.
- Source-order multi-step method chains.
- Intermediate validation.
- Input, intermediate, and output validation modes.
- Documented data-quality constraint boundary with schema-only validation as the default.
- `where(...)` filtering.
- `@special(type="expr")` helpers.
- `@raw(lane=lane)` and `@raw(lane=lane)` hooks.
- Hook signature validation.
- `lookup_join(...)`.
- N-step serial joins.
- Clean no-hook generated code.
- Structured compiler errors.
- Streaming compatibility checks.
- Streaming compatibility report.
- Compiler provenance from source node to IR node to generated PySpark node.
- Static dataflow traceability inferred from IR.
- TOML configuration hardening.
- Diagnostic codes with documentation links.
- Setup/configuration doctor.
- Incremental-compile architecture hooks.

## Phase 3: v2

- Windowing.
- Deduplication helpers.
- Aggregations.
- Advanced grouping.
- Spark higher-order functions.
- Caching and persistence annotations.
- Repartition/coalesce annotations.
- Join strategy annotations.
- `inner_join(...)` and other row-multiplying or existence-oriented join forms.
- Opt-in data-quality constraint model.
- Phase-bound data-quality constraint execution.
- Richer static dataflow explain output.
- Generated documentation artifacts for schemas and transforms.
- Pytest helper or plugin.

## Phase 4: v3

- Planned Column API and SQL function PySpark parity gaps.
- Using-key joins.
- Right and full join diagnostics hardening.
- Cross join safety.
- Join strategy directives.
- Forward as-of joins.
- Explicit grouping sets.
- Post-aggregate `having(...)`.
- Window null ordering.
- Multiple order keys across window helpers.
- Aggregate windows.
- Collection size and membership helpers.
- Array construction, repeat, union, and except helpers.
- Element lookup, safe element lookup, map-key membership, and map concatenation helpers.
- Caller-owned streaming transformation evidence and lifecycle guidance.

## Phase 5: v4

- Build a checked catalog for the PySpark 3.5.x/4.0.x transformation API baseline.
- Extend typed expression, nested-value, relational, join, aggregation, window, and collection coverage in dependency
  order.
- Admit row generators only after their schema-and-cardinality contract is proven.
- Keep loading, storage, actions, orchestration, alternative backends, and non-batch Spark Connect work out of v4.

## Build Integration

Initial build integration should rely on CLI commands:

```bash
structure check
structure compile
structure compile --fail-on-diff
```

Later, add optional pytest and build-tool integrations as v2 adoption tooling. Production incremental compile is a
separately planned future item after the transformation coverage program.

Compiler build integration must stay Spark-free. `structure check`, `structure compile`, and
`structure compile --fail-on-diff` must not require PySpark, Java, a SparkSession, or a Spark cluster. Direct runtime,
generated-code import, and PySpark execution tests may require those dependencies and should remain separate from
compiler checks.

## Compile-Time Performance Metrics

Track:

- config load time
- module discovery time
- source inspection time
- symbolic execution time
- check time
- codegen time
- formatting time
- compiler provenance time
- static dataflow traceability time
- files regenerated
- cache hit ratio
- total wall-clock time

Add `structure compile --profile` to emit these measurements. Production incremental compilation remains future work;
v1/v2 should preserve deterministic outputs and source fingerprints so the cache can be added without reshaping the
compiler.
