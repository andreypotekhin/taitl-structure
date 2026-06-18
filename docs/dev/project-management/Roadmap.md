# Roadmap

## Product Direction

Structure is a compiler-first data pipeline library. Developers write typed, schema-returning Python transform classes.
Structure compiles them to generated PySpark classes using DataFrame and Column operations so Spark can optimize
execution.

The north star is deliberately strict: v1 proves that Structure can replace hand-maintained PySpark boilerplate with a
strict, readable compiler workflow. v2 makes that workflow useful for mainstream analytical pipelines. v3 takes
ownership of streaming lifecycle concerns only after the transform compiler has earned trust. v4 adds backend expansion
through Spark Connect after the ordinary PySpark contract is stable.

The project should prioritize:

- Fast, predictable compilation.
- Clean generated PySpark code.
- Strong schema enforcement.
- IDE-friendly source authoring.
- Strict performance guardrails.
- Actionable compiler diagnostics.
- Incremental delivery through working vertical slices.

## Pre-Coding Gate

Before the first vertical slice, Sprint 00 must retire the highest-risk unknowns called out in
`docs/dev/design/Challenges.md`.

Required spikes:

- Prove `@after(method)` binding inside class bodies.
- Prove class-local `@expr_fn` helpers callable through `self` without a `self` parameter.
- Prove source-order discovery with stable line numbers.
- Prove source-root discovery and generated `structure_generated.<source package>` import paths.
- Prove compiler checks and compile can run without PySpark, Java, SparkSession, Spark startup, or a Spark cluster.
- Prove a minimal generated PySpark execution test with local Spark.
- Document and wire the v1 compatibility policy before packaging decisions harden.

By default, Structure should use `src` when it contains importable packages and otherwise use the project root.
Generated code should live under `generated/structure_generated` and mirror source package paths below that
namespace. Other layouts remain configurable.

## v1 Scope

v1 focuses on schema-driven projection, filtering, joins, hooks, generated PySpark classes, validation, compiler
provenance, static dataflow lineage, and build integration.

### v1 must include

- `@transform` class discovery.
- `input(Schema)` declarations.
- Public schema-returning methods as source-ordered subtransforms.
- Schema base overlay construction for inherited output rows.
- One generated PySpark class per transform class.
- Generated `run(...)` methods.
- Optional generated convenience functions.
- Spark `StructType` generation.
- Primitive, array, map, and nested struct schema types.
- Runtime `assert_schema(...)` and `project_schema(...)`.
- Intermediate schema validation by default.
- Symbolic expression execution.
- `where(...)` filtering.
- `@expr_fn` helpers.
- `@before(method)` and `@after(method)` hooks.
- Clean generated code with no hook machinery for hook-free transforms.
- `join_one(...)` symbolic joins.
- N-step serial joins across arbitrary named inputs.
- Compiler provenance from source node to IR node to generated PySpark node.
- Static dataflow lineage inferred from IR for transform, table, and column dependencies.
- CLI `check`, `compile`, `explain`.
- Small TOML configuration with seed defaults, explicit resolution order, and schema validation diagnostics.
- Python 3.11+ and PySpark 3.5.x/4.0.x compatibility policy.
- Build/CI support including `--fail-on-diff`.
- Spark-free compiler commands for `check`, `compile`, and `compile --fail-on-diff`.
- Streaming-compatible generated transforms when Spark operations support streaming inputs.
- Streaming compatibility reporting that explains whether a transform is compatible, batch-only, or unknown.
- Diagnostic codes and documentation links for setup, import safety, PySpark targeting, generated-code drift,
  validation, and compileability issues.
- A `structure doctor` command or equivalent setup/configuration check.

## v2 Scope

v2 extends the compiler IR and emitter with advanced Spark operations while preserving performance discipline.

### v2 candidate features

- Windowing for dedupe, latest-row, ranking, lag/lead, and rolling metrics.
- Deduplication helpers.
- Aggregations and typed `group_by(...)`.
- Advanced aggregation and grouping sets where practical.
- Spark higher-order functions for arrays/maps where compiler-visible.
- Manual optimization directives: caching, persistence, repartition/coalesce, checkpoint hints.
- Advanced join strategies: broadcast, shuffle hash, sort merge hints, lookup projection, prejoin dedupe warnings.
- `join_many(...)`, semi/anti joins, and other row-multiplying or existence-oriented join forms.
- Production incremental compilation: `compile --changed-only`, cache invalidation policies, and cache diagnostics.
- Richer static dataflow explain output.
- More complete generated-code explain reports.
- Generated documentation artifacts for schemas and transforms, in Markdown or JSON.
- Pytest helper or plugin for `structure check`, generated-code freshness, and generated-code snapshots.

## v3 Scope

v3 introduces streaming orchestration. v1/v2 only maintain streaming compatibility when callers pass streaming
DataFrames.

### v3 candidate features

- Generated `readStream` and `writeStream` code.
- Streaming sinks/sources configuration.
- Trigger configuration.
- Checkpoint configuration.
- Output mode configuration.
- Watermarks and state policies.
- Full streaming job generation.

## v4 Scope

v4 adds Spark Connect support after the ordinary PySpark generated-code contract and streaming orchestration semantics
are stable.

### v4 candidate features

- Spark Connect generated-code contract.
- Spark Connect compatibility tests.
- Backend capability reporting for ordinary PySpark and Spark Connect targets.
- Public migration notes for projects that want Connect-compatible generated code.

## Release Milestones

| Milestone | Goal | Sprints |
|---|---|---|
| M0 | Repository, compiler skeleton, and pre-coding spike gate | Sprint 00 |
| M1 | First end-to-end generated PySpark transform | Sprint 01 |
| M2 | Schema validation and generated class polish | Sprint 02 |
| M3 | Practical expression DSL and diagnostics | Sprint 03 |
| M4 | Hook model and no-hook generated-code cleanliness | Sprint 04 |
| M5 | Joins, compiler lineage, build integration | Sprint 05 |
| M6 | v1 stabilization and docs/examples | follow-up hardening sprint |
| M7 | v2 analytical pipeline features and adoption tooling | future v2 sprints |
| M8 | v3 streaming orchestration | future v3 sprints |
| M9 | v4 Spark Connect backend expansion | future v4 sprints |
