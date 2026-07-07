# Roadmap

## Product Direction

Structure is an IR-first data pipeline library. Developers write typed, schema-returning Python transform classes.
Structure runs them online by default through `StructureSession`, and can also emit generated PySpark classes using
DataFrame and Column operations so Spark can optimize execution.

The north star is deliberately strict: v1 first proves one useful executable transform running both online and as
generated PySpark, then broadens into the contract that lets Structure replace hand-maintained PySpark boilerplate
with a strict online runtime and optional generated-code workflow. v2 makes that workflow useful for mainstream
analytical pipelines.
v3 takes ownership of streaming lifecycle concerns only after the transform compiler has earned trust. Sprint 09 moves
Spark Connect from experimental parity to supported status for completed batch features and completes the static
caller-owned streaming compatibility contract; live streaming runtime evidence remains a v3 entry gate. v4 remains
available for non-batch Connect hardening and broader backend expansion.

The project should prioritize:

- Fast, predictable compilation.
- Online PySpark execution by default.
- Clean optional generated PySpark code.
- Strong schema enforcement.
- IDE-friendly source authoring.
- Strict performance guardrails.
- Actionable compiler diagnostics.
- Incremental delivery through working vertical slices.

## Pre-Coding Gate

Before the first vertical slice, Sprint 00 must retire the highest-risk unknowns called out in
[Challenges.md](../design/Challenges.md).

Required spikes:

- Prove `@after(method, lane=lane)` binding inside class bodies.
- Prove class-local `@expr_fn` helpers callable through `self` without a `self` parameter.
- Prove source-order discovery with stable line numbers.
- Prove source-root discovery and generated `structure_generated.<source package>` import paths.
- Prove `StructureSession` and deferred transform invocation API.
- Prove shared PySpark execution recipes for projection-only online/generated parity.
- Prove compiler checks and compile can run without PySpark, Java, SparkSession, Spark startup, or a Spark cluster.
- Prove a minimal generated PySpark execution test with local Spark.
- Document and wire the v1 compatibility policy before packaging decisions harden.

By default, Structure should use `src` when it contains importable packages and otherwise use the project root.
Generated code should live under `generated/structure_generated` and mirror source package paths below that
namespace. Other layouts remain configurable.

## Sprint 01 Scope

Sprint 01 proves the first executable v1 contract. It gives the team a narrow runnable path before the larger v1 scope
hardens.

### Sprint 01 must include

- `@transform` class discovery.
- `input(Structure)` declaration for one named input.
- `StructureSession`.
- Builder-style transform invocation.
- Online PySpark runner for the first v1 fixture.
- Shared PySpark execution semantic contract for online/generated parity.
- One public schema-returning method.
- One generated PySpark class and convenience function.
- Spark `StructType` generation for the first v1 schemas.
- Runtime input `assert_schema(...)`.
- Symbolic field references.
- Projection.
- `where(...)` filtering.
- One `@expr_fn` helper.
- Online/generated parity test.
- Spark-free `structure check` for the fixture.

### Sprint 01 excludes

- joins;
- hooks;
- compiler provenance and static dataflow traceability;
- streaming compatibility reporting;
- setup/configuration doctor checks;
- build integration such as `compile --fail-on-diff`;
- production incremental compile hooks.

## v1 Scope

v1 focuses on schema-driven online execution, projection, filtering, joins, hooks, optional generated PySpark classes,
validation, compiler provenance, static dataflow traceability, and build integration.

### v1 must include

- `@transform` class discovery.
- `input(Structure)` declarations.
- `StructureSession`.
- Builder-style transform invocation.
- Online PySpark runner.
- Runtime target registry for online and generated PySpark execution.
- Shared PySpark execution semantic contract for online/generated parity.
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
- `@before(method, lane=lane)` and `@after(method, lane=lane)` hooks.
- Clean generated code with no hook machinery for hook-free transforms.
- `lookup_join(...)` symbolic joins.
- N-step serial joins across arbitrary named inputs.
- Compiler provenance from source node to IR node to generated PySpark node.
- Static dataflow traceability inferred from IR for transform, table, and column dependencies.
- CLI `check`, `compile`, `explain`.
- Small TOML configuration with seed defaults, explicit resolution order, and schema validation diagnostics.
- Python 3.11+ and PySpark 3.5.x/4.0.x compatibility policy.
- Build/CI support including `--fail-on-diff`.
- Spark-free compiler commands for `check`, `compile`, and `compile --fail-on-diff`.
- Streaming-compatible online and generated transforms when Spark operations support streaming inputs.
- Streaming compatibility reporting that explains whether a transform is compatible, batch-only, or unknown.
- Diagnostic codes and documentation links for setup, import safety, PySpark targeting, generated-code drift,
  validation, and compileability issues.
- A `structure doctor` command or equivalent setup/configuration check.

## v2 Scope

v2 makes Structure useful for mainstream analytical batch pipelines after the v1 compiler contract is stable. The
release broadens the IR, shared PySpark recipe layer, online runner, generated emitter, diagnostics, and tests without
changing the core authoring model: developers still write schema-returning transform methods, Structure still keeps
supported logic Spark-plan-visible, and hooks remain explicit escape hatches.

### v2 release pillars

- **Analytical transforms:** typed `group_by(...)`, first-slice aggregations, selected-row helpers, deduplication,
  ranking, lag/lead, rolling metrics, compiler-visible Spark higher-order functions for arrays and maps, and advanced
  analytical coverage from [AdvancedAnalyticalOperations.md](../specifications/AdvancedAnalyticalOperations.md).
- **Analytical joins:** existence predicates, `inner_join(...)`, deterministic lookup dedupe, temporal validity-window
  joins, and backward as-of joins from [AnalyticalJoinCoverage.md](../specifications/AnalyticalJoinCoverage.md).
- **Full PySpark rowset joins:** right joins, full joins, cross joins, non-equi predicates, and disjunctive predicates
  from [FullPySparkJoinSupport.md](../specifications/FullPySparkJoinSupport.md).
- **Explicit optimization controls:** cache/persist first-slice directives are admitted in Sprint 09. Repartition,
  coalesce, checkpoint, and broader join strategy directives remain deferred until their physical-plan contract is
  specified.
- **Transform composition maturity:** hook-bearing stages, composed hook ownership and dispatch, traceability for
  composed hook boundaries, and explicit decisions on earlier-stage output exposure and mixed wrapper-local logic.
- **Adoption and scale tooling:** generated documentation artifacts for schemas and transforms, pytest helpers,
  generated-code freshness checks, snapshots, richer generated-code explain reports, and production incremental
  compilation with cache diagnostics move to Sprint 10 and later adoption-tooling work.

### v2 non-goals

- Full streaming orchestration. v2 supports only caller-owned streaming DataFrames in the first streaming slice and
  maintains compatibility classification for everything else.
- Spark Connect streaming orchestration or storage write ownership. Sprint 09 owns batch support promotion only.
- Automatic cost-based optimization, join reordering, or storage write planning.
- Hidden UDF lowering or arbitrary Python execution in compiled paths.
- Using `lane(...)` as a transform-composition matching boundary.

## v3 Scope

v3 introduces streaming orchestration and advanced optimizer work.
v1/v2 only support the first caller-owned streaming DataFrame slice and maintain streaming compatibility
classification for broader streaming behavior.

### v3 candidate features

- Join reordering and cost-based join planning.
- Lateral joins and table-valued-function joins if a later PySpark design admits them.
- Generated `readStream` and `writeStream` code.
- Streaming sinks/sources configuration.
- Trigger configuration.
- Checkpoint configuration.
- Output mode configuration.
- Watermarks and state policies.
- Full streaming job generation.

## v4 Scope

v4 handles backend expansion after the completed PySpark-family batch contract is stable. Spark Connect may continue
there only for non-batch coverage, expanded operational hardening, or gaps that Sprint 09 explicitly records as outside
the supported batch surface.

### v4 candidate features

- Ibis or other meta-backend exploration.
- Expanded Spark Connect streaming, storage-write, or operational hardening if needed.
- Backend capability reporting for additional backend families.
- Public migration notes for projects adopting new backend families.

## Release Milestones

| Milestone | Goal | Sprints |
|---|---|---|
| M0 | Repository, compiler skeleton, and pre-coding spike gate | Sprint 00 |
| M1 | first executable slice | Sprint 01 |
| M2 | Schema validation and generated class polish | Sprint 02 |
| M3 | Practical expression DSL and diagnostics | Sprint 03 |
| M4 | Hook model and no-hook generated-code cleanliness | Sprint 04 |
| M5 | Joins, compiler traceability, build integration | Sprint 05 |
| M6 | v1 stabilization and docs/examples | follow-up hardening sprint |
| M7 | v2 analytical pipeline features, analytical join coverage, composition maturity, adoption tooling, and Spark Connect batch support | Sprints 06-09 |
| M8 | v3 streaming orchestration | future v3 sprints |
| M9 | v4 backend expansion and non-batch Spark Connect hardening | future v4 sprints |
