# Product Backlog

## Epic: Pre-Coding Spikes and Decisions

- SPIKE: Prove `@after(method, lane=lane)` works inside class bodies.
- SPIKE: Prove class-local `@expr_fn` helpers work without a `self` parameter.
- SPIKE: Prove source-order method discovery with stable line numbers.
- SPIKE: Prove source-root discovery and generated `structure_generated.<source package>` import paths.
- SPIKE: Prove `StructureSession` and deferred transform invocation API.
- SPIKE: Prove `structure check` and `structure compile` can run without PySpark, Java, SparkSession, Spark startup,
  or a Spark cluster.
- SPIKE: Prove a minimal generated PySpark execution test with local Spark.
- Implement the documented schema declaration syntax from `SchemaDeclarationSyntax.spec.md`.
- Implement nullability and coercion rules from `NullabilityAndTypeCoercion.spec.md`.
- Decide and document generated-code ownership rules before CI integration.
- Implement compatibility, versioning, and license policy before open-source packaging.

## Epic: Project Foundation

- Create Python package skeleton.
- Add CLI entrypoint.
- Add TOML configuration loader.
- Add explicit config resolution order: CLI flags, `[tool.structure]` in `pyproject.toml`, `structure.toml`, defaults.
- Add config schema validation for unknown keys and invalid values.
- Add structured config diagnostics with allowed values for enum-like settings.
- Add seed config generator.
- Add CI test workflow.
- Add formatting/linting setup.
- Add generated directory conventions.
- Add `execution_mode = "online"` default.

## Epic: Schema Model

- Implement `Structure` base class.
- Implement field definitions.
- Implement primitive scalar types.
- Implement explicit type objects: `String()`, `Float()`, `Double()`, `Decimal(...)`, `Array(...)`, `Map(...)`, and
  `Struct(...)`.
- Implement schema inheritance semantics from `SchemaInheritance.spec.md`.
- Implement nullable metadata.
- Implement Spark `StructType` emitter.
- Implement schema equality and compatibility checks.
- Implement generated schema modules.

## Epic: Transform Discovery

- Implement `@transform` decorator.
- Discover transform classes under configured source directory.
- Preserve class member source order.
- Detect `input(...)` declarations.
- Identify public schema-returning methods.
- Identify `@expr_fn` helpers.
- Identify `@before(method, lane=lane)` and `@after(method, lane=lane)` hooks.
- Report ambiguous public methods.

## Epic: Symbolic Execution

- Implement symbolic row proxies.
- Implement field reference expressions.
- Implement literal expressions.
- Implement unary/binary expression operators.
- Implement schema output construction capture.
- Implement `where(...)` predicate capture.
- Implement expression helper symbolic execution.
- Implement unsupported-operation diagnostics.

## Epic: IR and Checks

- Define `TransformPlan`.
- Define `StepPlan`.
- Define expression IR.
- Define plan operations: `Filter`, `Project`, `Join`, `HookCall`, `ValidateSchema`.
- Validate source-order schema flow.
- Validate expression type compatibility.
- Validate `where(...)` predicates are boolean.
- Validate hook signatures.
- Validate performance guardrails.

## Epic: PySpark Code Generation

- Generate transform classes.
- Generate convenience functions.
- Generate schema modules.
- Generate `select(...)` projections.
- Generate `where(...)` filters.
- Generate `join(...)` operations.
- Generate intermediate validation.
- Generate hook calls only when needed.
- Generate formatted deterministic code.

## Epic: Online Execution Runtime

- Implement `StructureSession`.
- Implement builder-style transform invocation input binding.
- Implement runtime target registry.
- Implement online PySpark runner.
- Implement generated PySpark runner.
- Add online/generated parity tests.

## Epic: Runtime Support

- Implement `assert_schema(...)`.
- Implement `project_schema(...)`.
- Implement `PipelineContext`.
- Implement schema comparison utilities.
- Add runtime tests independent of compiler internals.

## Epic: Hooks

- Implement `@before(method, lane=lane)` metadata.
- Implement `@after(method, lane=lane)` metadata.
- Validate selected lane signatures such as `def hook(self, *, orders, spark, ctx)`.
- Generate direct source hook calls.
- Support schema mode and project-output options.
- Ensure no hook machinery in hook-free generated code.

## Epic: Joins

- Implement symbolic named input scopes.
- Implement `join_one(...)`.
- Implement join type enum.
- Implement join hint enum.
- Generate aliases predictably.
- Support arbitrary N-step serial joins.
- Warn when `join_one(...)` lacks uniqueness metadata.

## Epic: Compiler Traceability

- Add compiler provenance from source node to IR node to generated PySpark node.
- Add static dataflow traceability inferred from IR.
- Track transform, named input, step, schema, field, join, filter, expression helper, and hook-boundary dependencies.
- Surface provenance and static dataflow in compiler diagnostics.
- Add `structure explain` traceability output for transform, step, and field dependencies.
- Add streaming compatibility reporting with compatible, batch-only, and unknown states.
- Add a registry-backed diagnostic code and documentation contract.
- Add `structure doctor` or equivalent setup/configuration checks.

## Epic: Build Integration

- Implement `structure check`.
- Implement `structure compile`.
- Implement `structure explain`.
- Implement `structure compile --fail-on-diff`.
- Add no-Spark guard tests for compiler commands.

## v2 Backlog

### Epic: v2 Scope and Analytical IR Foundations

- Publish v2 release scope, non-goals, user stories, milestones, and sprint charters.
- Add v2 fixture package for aggregation, window, dedupe, higher-order function, optimization hint, and analytical join
  examples.
- Extend IR operation taxonomy for aggregation, window, higher-order function, optimization directive, and analytical
  join operations.
- Record source location, backend capability, cardinality, streaming compatibility, and traceability metadata on every
  v2 operation.
- Add backend capability names for aggregation, window, higher-order function, optimization directive, and analytical
  join forms.
- Add diagnostic codes and public documentation anchors for unsupported v2 operation shapes.
- Add online/generated parity harness fixtures for v2 operations before implementing the full lowering set.

### Epic: Aggregations and Grouping

- Implement typed `group_by(...)` source DSL.
- Implement aggregation expression builders for count, sum, min, max, average, and distinct count where practical.
- Support aggregate output schema construction with grouped keys and aggregate fields.
- Validate aggregate expressions against input field types and nullable output expectations.
- Lower group-by and aggregate plans through shared PySpark recipes.
- Add generated PySpark snapshots for aggregate transforms.
- Add online/generated parity tests for grouped rollups.
- Stage advanced grouping sets, rollups, and cubes behind explicit backend capability checks.

### Epic: Windowing and Deduplication

- Implement window specification objects with partitioning, ordering, and frame boundaries.
- Implement ranking helpers.
- Implement lag and lead helpers.
- Implement rolling metric helpers.
- Implement latest-row and earliest-row helpers with deterministic tie policies.
- Implement duplicate-removal helpers for exact duplicate removal and selected-row dedupe.
- Reject nondeterministic selected-row dedupe unless the source declares an explicit tie policy.
- Lower window and dedupe plans through shared PySpark recipes.
- Add online/generated parity tests for ranking, lag/lead, rolling metrics, latest-row, and duplicate-removal scenarios.

### Epic: Higher-Order Array and Map Functions

- Implement compiler-visible array helper forms for transform, filter, exists, forall, aggregate, and zip-with where
  supported by the configured PySpark target.
- Implement compiler-visible map helper forms for key/value transform and map filter where supported.
- Validate higher-order helper callbacks as symbolic expressions, not arbitrary Python callbacks.
- Emit actionable diagnostics that suggest `@expr_fn` or hooks when a helper cannot remain compiler-visible.
- Lower higher-order helper plans through shared PySpark recipes.
- Add online/generated parity tests for arrays, maps, nullable elements, and unsupported callback diagnostics.

### Epic: Analytical Joins

- + Implement semi `exists(...)` and anti `not_exists(...)` predicates.
- + Implement `join_many(...)` for row-multiplying joins.
- + Implement deterministic `JoinDedupe.latest_by(...)` and `JoinDedupe.earliest_by(...)` policies.
- Implement temporal validity-window `temporal_one(...)` joins for SCD-style lookups.
- Implement backward `as_of_one(...)` joins with optional tolerance.
- Add tie and overlap policy diagnostics.
- Show analytical join cardinality in traceability and `structure explain`.
- Add online/generated parity tests for duplicate right rows, unmatched rows, temporal overlaps, and as-of ties.

### Epic: Explicit Optimization Directives

- Implement cache and persist directives at subtransform boundaries.
- Implement repartition and coalesce directives.
- Implement checkpoint hints where supported by the configured backend.
- Implement join strategy directives for broadcast, shuffle hash, sort merge, and lookup projection where supported.
- Keep directives explicit in source, IR, generated code, and explain output.
- Add diagnostics when a directive is unsafe, unsupported, or likely ignored by the configured PySpark target.
- Add tests proving directives do not change row or schema semantics.

### Epic: Explain, Documentation, and Test Tooling

- Add rich `structure explain` mode for field-level lineage through projections, filters, joins, aggregations, windows,
  hooks, and optimization boundaries.
- Add generated documentation artifacts for schemas and transforms in Markdown or JSON.
- Add pytest helpers for `structure check`, generated-code freshness, generated-code snapshots, expected diagnostics,
  and online/generated parity.
- Add production incremental compilation with `compile --changed-only`.
- Add cache invalidation policies and cache diagnostics for source, config, schema, dependency, and generated-target
  changes.
- Add performance tests for incremental compile on synthetic 10-transform and 100-transform projects.

## v3 Backlog

- Streaming source declarations.
- Streaming sink declarations.
- Generated `readStream`.
- Generated `writeStream`.
- Triggers.
- Checkpoints.
- Output modes.
- Watermarks.
- Stateful streaming policies.

## v4 Backlog

- Spark Connect support.
- Spark Connect compatibility tests.
- Backend capability reporting for ordinary PySpark and Spark Connect targets.

## Nice To Have Beyond v4

- Runtime LDJSON traceability emitter. See [NiceToHave.md](NiceToHave.md).
