# Roadmap

## Product Direction

Structure is an IR-first data pipeline library. Developers write typed, schema-returning Python transform classes.
Structure runs them online by default through `StructureSession`, and can also emit generated PySpark classes using
DataFrame and Column operations so Spark can optimize execution.

The north star is deliberately strict: v1 first proves one useful executable transform running both online and as
generated PySpark, then broadens into the contract that lets Structure replace hand-maintained PySpark boilerplate
with a strict online runtime and optional generated-code workflow. v2 makes that workflow useful for mainstream
analytical pipelines.
v3 hardens streaming transformations while lifecycle ownership remains caller-owned. Sprint 09 moves
Spark Connect from experimental parity to supported status for completed batch features and completes the static
caller-owned streaming compatibility contract; live streaming runtime evidence remains a v3 entry gate. v4 focuses on
PySpark transformation coverage. v5 pivots target ownership to a public platform callback architecture: Core retains
every workflow engine by default, while bundled and external platforms supply target-specific callbacks. A selected
platform may privately replace a compatible individual engine for its own target; this is not a public extension API.

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

- Prove `@raw(lane=lane)` binding inside class bodies.
- Prove class-local `@special(type="expr")` helpers callable through `self` without a `self` parameter.
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
- One `@special(type="expr")` helper.
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
- Public schema-returning methods as source-ordered step methods.
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
- `@special(type="expr")` helpers.
- `@raw(lane=lane)` and `@raw(lane=lane)` hooks.
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
  generated-code freshness checks, snapshots, and richer generated-code explain reports move through Sprint 10.
  Production incremental compilation with cache diagnostics remains future work after the v4 feature surface stabilizes.

### v2 non-goals

- Full streaming orchestration. v2 supports only caller-owned streaming DataFrames in the first streaming slice and
  maintains compatibility classification for everything else.
- Spark Connect streaming orchestration or storage write ownership. Sprint 09 owns batch support promotion only.
- Automatic cost-based optimization, join reordering, or storage write planning.
- Hidden UDF lowering or arbitrary Python execution in compiled paths.
- Using `lane(...)` as a transform-composition matching boundary.

## v3 Scope

v3 closes its scheduled PySpark parity gaps, then hardens admitted streaming transformations while callers retain
lifecycle ownership.
v1/v2 support compiler-visible batch features, Spark Connect batch execution for completed features, and static
caller-owned streaming compatibility. v3 starts by broadening the typed symbolic surface so streaming diagnostics and
generated lifecycle code can rely on a complete enough PySpark-family contract.

### v3 beginning-of-release sequence

- Sprint 11: DSL and SQL function PySpark parity.
- Sprint 12: join PySpark parity hardening.
- Sprint 13: aggregation PySpark parity.
- Sprint 14: window PySpark parity.
- Sprint 15: higher-order and collection helper PySpark parity.
- Sprint 16: streaming transformation hardening.

### v3 must include

- Planned Column API and SQL function gaps from [Gaps.md](../Gaps.md).
- Using-key joins, right/full diagnostics hardening, cross join safety, supported join strategy directives, and forward
  as-of joins.
- Explicit grouping sets and post-aggregate `having(...)`.
- Null ordering in window order keys, normalized multiple order keys, and aggregate windows.
- Collection size, array and map membership, array construction/repeat/union/except, element lookup, safe element
  lookup, and map concatenation.
- Compiler-visible watermarks, stateful streaming dedupe, streaming aggregations, and compatibility diagnostics.
- Caller-owned file-stream evidence and guidance for sources, sinks, triggers, checkpoints, output modes, and query
  lifecycle.

### v3 non-goals

- Wholesale PySpark wrapper behavior.
- Raw SQL expressions, raw PySpark `WindowSpec`, UDTF helpers, and arbitrary Python callbacks. Scalar
  `@special(type="udf")` remains supported for ordinary batch PySpark with its warning policy, but is excluded from
  Structure's streaming contract.
- Cost-based join reordering, nearest as-of joins, and lateral or table-valued-function joins until a dedicated plan
  admits their contracts.
- Row-expanding generator helpers unless a separate cardinality design admits them.
- Custom streaming side-effect sinks such as `foreachBatch`.

## v4 Scope

v4 expands predictable PySpark transformation API coverage. Its first delivery is a checked coverage catalog for the
PySpark 3.5.x/4.0.x target intersection; later slices admit the highest-value remaining Column, SQL-function, nested,
relational, join, aggregation, window, and collection operations. The release deliberately stays within transformations
over caller-supplied DataFrames.

### v4 sequence

- Sprint 17: transformation API coverage foundation and catalog.
- Scalar and conditional expression coverage.
- Nested values and declared parsing coverage.
- Relational transformations and advanced analytical coverage.
- Sprint 18: caller-owned streaming migration for session-window aggregation, bounded stream-stream outer and semi
  joins, and stream-static left-semi joins. The slice preserves caller ownership of sources, sinks, checkpoints,
  triggers, output-mode application, and query lifecycle.
- A gated row-generator design and implementation slice.
- Final v4 hardening sprint: release evidence, documentation closure, and resolution or deferral of release-blocking
  defects after every v4 feature slice.

### v4 non-goals

- Loading, storage, writes, catalog/table management, and DataFrame actions.
- Streaming sources, sinks, triggers, checkpoints, output-mode application, and query lifecycle.
- Unbounded state, chained stateful operators, opaque Pandas/RDD/state-processor APIs, and arbitrary streaming side
  effects.
- Raw SQL, raw `WindowSpec`, UDTF helpers, and arbitrary Python callback behavior. Scalar `@special(type="udf")`
  remains ordinary-PySpark batch support only.
- Alternative backend expansion and non-batch Spark Connect work.
- Cost-based join reordering without a separate optimizer design.

## v5 Scope

v5 separates Structure Core from target semantics without turning plugins into replacement applications. Core owns
schema processing, compilation, execution, generation, serialization, capability reporting, diagnostics, artifact
management, and CLI orchestration. Platform plugins participate through one provider and one negotiated `PlatformAPI1`
façade, at the highest version supported by Core and that provider.

### v5 sequence

- Sprint 19: specify and implement discovery, provider manifests, API negotiation, target resolution, callback
  contracts, and Core-owned artifact envelopes.
- Sprint 20: refactor PySpark authoring, schema, compiler, execution, generation, and capability behavior behind the
  public callbacks while preserving current runtime and generated-code evidence.
- Sprint 21: publish the external plugin author contract and conformance kit; build the internal finite-iterable wheel
  to prove real package discovery, execution, serialization, disabling, and isolation.
- Sprint 22: remove legacy PySpark root exports and backend-specific Core paths, migrate documentation and fixtures,
  run compatibility evidence, and harden v5 for release.

### v5 must include

- Exactly one target per transform or composed pipeline; different transforms in one project may use different
  installed targets.
- Metadata-only package entry-point discovery, with all installed distributions eligible by default and explicit
  distribution disabling for conflicts.
- Short platform identifiers with deterministic duplicate-provider diagnostics.
- Explicit minimum/maximum Platform API versions and highest-mutual-version negotiation in both downgrade directions.
- Core-owned workflows with independent schema, compiler, capability, execution, generation, and serialization
  callback-provider entry points.
- Target-owned public field definitions and transformation grammar; no permanent root aliases for PySpark APIs.
- A generic `StructureSession(runtime=..., context=...)` runtime boundary.
- Standard Core diagnostic and capability records whose target-specific content is supplied by plugins.
- A bundled `structure.platform.pyspark` implementation with no private integration path.
- Supported external plugin development through vendor-owned packages, public documentation, and conformance tests.
- An internal finite-iterable plugin supporting projection, inner/left joins, grouped sum/count, re-iterable results,
  `collect()`, and opaque-plan serialization.

### v5 non-goals

- Source compatibility between platform-specific transforms.
- More than one target in a transform or composed pipeline.
- Cross-platform data exchange, pipeline handoff, or automatic API translation.
- A generic plugin message bus or arbitrary plugin-defined Core workflows.
- Production support or public end-user documentation for the finite-iterable conformance plugin.
- New PySpark transformation families unrelated to completing the platform extraction.

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
| M8 | v3 PySpark gap closure and streaming transformation hardening | Sprints 11-16 |
| M9 | v4 PySpark transformation API coverage | Sprint 17, later v4 feature sprints including Sprint 18 streaming migration, then the final v4 hardening sprint |
| M10 | v5 Core-orchestrated platform callback architecture | Sprints 19-22 |
