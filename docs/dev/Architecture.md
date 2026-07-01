# Architecture

Structure is an IR-first runtime/compiler toolkit for schema-driven data pipelines. PySpark is the v1 runtime and
generation target, but the compiler core is designed to remain backend-neutral.

It is not intended to be a heavy runtime framework. Source DSL files compile to backend-neutral IR. In v1, that IR can
be consumed by the online PySpark runner at runtime or by the PySpark code generator to emit optional generated classes.

## Goals

- Schema-first authoring.
- IDE-friendly source code.
- Spark optimizer-visible online and generated execution for the v1 target.
- Explicit arbitrary target-scoped hooks.
- Clean hook-free generated code.
- Lightweight runtime session.
- Fast compiler feedback.
- Version-aware PySpark target boundary.

## High-Level Data Flow

```text
src/orders/
  schemas/
  transforms/

        -> compiler frontend

compiler
  config
  discovery
  symbolic execution
  compileability checks
  IR generation
  compiler provenance
  static dataflow traceability

        -> online execution

StructureSession
  OnlinePySparkRunner
  PySpark DataFrame operations

        -> optional structure compile

generated/structure_generated/
  orders/pyspark/
    schemas/
    transforms/
  runtime/
  traceability/  # compiler metadata, not runtime telemetry

        -> generated execution

Airflow / Spark job imports generated code when configured
```

## Major Components

- DSL
- Schema model
- Discovery and inspection
- Symbolic execution engine
- Intermediate representation
- Compileability checker
- Online execution runtime
- PySpark code generator
- Runtime support library
- CLI

Each component has a detailed design document under `docs/dev/design/`.

## Backend Boundary

The compiler produces backend-neutral IR. The online PySpark runner lowers IR to live PySpark DataFrame and Column
objects. The PySpark code generator lowers IR to concrete PySpark source code.

Online and generated PySpark execution share a target semantic contract. Checked `TransformPlan` IR plus
`PySparkCapabilities` lowers to deterministic PySpark execution recipes before either runtime path consumes it. The
online runner interprets those recipes with live PySpark objects. The generated emitter renders those same recipes as
source text. The contract is specified in [ExecutionSemanticContract.md](../specifications/ExecutionSemanticContract.md) and designed in
[ExecutionSemanticContract.md](design/ExecutionSemanticContract.md).

This boundary is important for keeping up with PySpark evolution. PySpark API compatibility should be isolated in the
PySpark target layer rather than scattered across discovery, symbolic execution, or checks. The exact boundary is the
backend capability interface specified in [BackendCapabilities.md](../specifications/BackendCapabilities.md) and designed in
[BackendCapabilities.md](design/BackendCapabilities.md).

Compiler phases must not depend on a live Spark installation. Discovery, schema extraction, symbolic execution,
compileability checks, IR construction, code generation, compiler provenance, static dataflow traceability, and
generated-file diff checks run without PySpark imports, Java, a SparkSession, or a Spark cluster. Online and generated
PySpark execution may depend on PySpark at runtime; the compiler itself must not.

Spark-free compilation does not mean Structure reimplements the whole PySpark API. The compiler owns a curated
semantic surface: schema projections, predicates, joins, validations, and later feature families such as aggregations,
windows, arrays, maps, and higher-order expressions. Each compiler-visible feature is represented as Structure DSL and
IR first, then lowered to PySpark target recipes. Rare, highly backend-specific, or arbitrary DataFrame logic remains
an explicit hook boundary instead of becoming a thin Structure wrapper around every Spark function.

The v1 default target is `target_pyspark = ">=3.5,<4.1"`, covering PySpark 3.5.x and 4.0.x. The PySpark target layer
should prefer the oldest clear optimizer-visible API inside the configured range. Unsupported backend targets and
unsupported feature requirements fail through `BACKEND-E2401` and `BACKEND-E2402`.

Alternative backend support must follow the same boundary. The compiler-visible Structure source should lower to
backend-neutral IR, then the selected target adapter checks capabilities and lowers that IR to a target execution plan
or generated artifact. Candidate future targets are Python-hosted: Spark SQL and typed PySpark DataFrame patterns
first, then Polars LazyFrame and DuckDB, then Ibis. Other targets should come through Ibis when practical. They must be
admitted by capability profile, target adapter, diagnostics, and tests rather than by ad hoc source rewrites. The design
is described in [AlternativeBackends.md](design/AlternativeBackends.md) and specified in
[AlternativeBackends.md](../specifications/AlternativeBackends.md).

Hooks are the exception to the same-source goal. Hook bodies are opaque runtime code and may rely on one backend's
DataFrame API. Future hook metadata must make that scope explicit through `target_backend` or an inherited
`hook_target_default`, and runtime execution must never invoke a hook against a backend outside its effective target
set.

Spark Connect belongs to v4 unless it can be supported through this target boundary without changing public DSL syntax,
online invocation construction, generated class construction, `run(...)` signatures, or streaming orchestration
semantics.

## Compile-Time Performance

Compile-time performance is a product metric.

The compiler should avoid Spark dependencies during normal `check` and `compile`. v1 should preserve deterministic
outputs and source fingerprints so production incremental compilation can arrive in v2 without reshaping the compiler.
