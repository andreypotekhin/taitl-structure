# Roadmap

## Product Direction

Structure is a compiler-first data pipeline library. Developers write typed, schema-returning Python transform classes. Structure compiles them to generated PySpark classes using DataFrame and Column operations so Spark can optimize execution.

The project should prioritize:

- Fast, predictable compilation.
- Clean generated PySpark code.
- Strong schema enforcement.
- IDE-friendly source authoring.
- Strict performance guardrails.
- Actionable compiler diagnostics.
- Incremental delivery through working vertical slices.

## Pre-Coding Gate

Before the first vertical slice, Sprint 00 must retire the highest-risk unknowns called out in `docs/dev/design/Challenges.md`.

Required spikes:

- Prove `@after(method)` binding inside class bodies.
- Prove class-local `@expr_fn` helpers callable through `self` without a `self` parameter.
- Prove source-order discovery with stable line numbers.
- Prove import paths using `structure_src` and `structure_generated`.
- Prove compiler checks can run without PySpark, SparkSession, Java, or Spark startup.
- Prove a minimal generated PySpark execution test with local Spark.

The default project layout is `structure_src/` for user source and `structure_generated/` for generated output. Other layouts remain configurable, but v1 planning should optimize for the safe defaults.

## v1 Scope

v1 focuses on schema-driven projection, filtering, joins, hooks, generated PySpark classes, validation, basic lineage, and build integration.

### v1 must include

- `@transform` class discovery.
- `input(Schema)` declarations.
- Public schema-returning methods as source-ordered subtransforms.
- One generated PySpark class per transform class.
- Generated `run(...)` methods.
- Optional generated convenience functions.
- Spark `StructType` generation.
- Runtime `assert_schema(...)` and `project_schema(...)`.
- Intermediate schema validation by default.
- Symbolic expression execution.
- `where(...)` filtering.
- `@expr_fn` helpers.
- `@before(method)` and `@after(method)` hooks.
- Clean generated code with no hook machinery for hook-free transforms.
- `join_one(...)` symbolic joins.
- N-step serial joins across arbitrary named inputs.
- Basic LDJSON lineage.
- CLI `check`, `compile`, `explain`.
- Small TOML configuration with seed defaults.
- Build/CI support including `--fail-on-diff`.
- Streaming-compatible generated transforms when Spark operations support streaming inputs.

## v2 Scope

v2 extends the compiler IR and emitter with advanced Spark operations while preserving performance discipline.

### v2 candidate features

- Aggregations and typed `group_by(...)`.
- Advanced aggregation and grouping sets where practical.
- Windowing for dedupe, latest-row, ranking, lag/lead, and rolling metrics.
- Spark higher-order functions for arrays/maps where compiler-visible.
- Manual optimization directives: caching, persistence, repartition/coalesce, checkpoint hints.
- Advanced join strategies: broadcast, shuffle hash, sort merge hints, lookup projection, prejoin dedupe warnings.
- Optional field-level LDJSON lineage.
- More complete generated-code explain reports.

## v3 Scope

v3 may introduce streaming orchestration. v1/v2 only maintain streaming compatibility when callers pass streaming DataFrames.

### v3 candidate features

- Generated `readStream` and `writeStream` code.
- Streaming sinks/sources configuration.
- Trigger configuration.
- Checkpoint configuration.
- Watermarks and state policies.
- Full streaming job generation.

## Release Milestones

| Milestone | Goal | Sprints |
|---|---|---|
| M0 | Repository, compiler skeleton, and pre-coding spike gate | Sprint 00 |
| M1 | First end-to-end generated PySpark transform | Sprint 01 |
| M2 | Schema validation and generated class polish | Sprint 02 |
| M3 | Practical expression DSL and diagnostics | Sprint 03 |
| M4 | Hook model and no-hook generated-code cleanliness | Sprint 04 |
| M5 | Joins, lineage, build integration | Sprint 05 |
| M6 | v1 stabilization and docs/examples | follow-up hardening sprint |
