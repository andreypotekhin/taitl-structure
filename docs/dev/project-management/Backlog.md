# Product Backlog

## Epic: Pre-Coding Spikes and Decisions

- SPIKE: Prove `@raw(lane=lane)` works inside class bodies.
- SPIKE: Prove class-local `@special(type="expr")` helpers work without a `self` parameter.
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
- Identify `@special(type="expr")` helpers.
- Identify `@raw(lane=lane)` and `@raw(lane=lane)` hooks.
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

- Implement `@raw(lane=lane)` metadata.
- Implement `@raw(lane=lane)` metadata.
- Validate selected lane signatures such as `def hook(self, *, orders, spark, ctx)`.
- Generate direct source hook calls.
- Support schema mode and project-output options.
- Ensure no hook machinery in hook-free generated code.

## Epic: Joins

- Implement symbolic named input scopes.
- Implement `lookup_join(...)`.
- Implement join type enum.
- Implement join hint enum.
- Generate aliases predictably.
- Support arbitrary N-step serial joins.
- Warn when `lookup_join(...)` lacks uniqueness metadata.

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

## v.2 Backlog

### Epic: v.2 Scope and Analytical IR Foundations

- Publish v.2 release scope, non-goals, user stories, milestones, and sprint charters.
- Add v.2 fixture package for aggregation, window, dedupe, higher-order function, optimization hint, and analytical join
  examples.
- Extend IR operation taxonomy for aggregation, window, higher-order function, optimization directive, and analytical
  join operations.
- Record source location, backend capability, cardinality, streaming compatibility, and traceability metadata on every
  v.2 operation.
- Add backend capability names for aggregation, window, higher-order function, optimization directive, and analytical
  join forms.
- Add diagnostic codes and public documentation anchors for unsupported v.2 operation shapes.
- Add online/generated parity harness fixtures for v.2 operations before implementing the full lowering set.

### Epic: Aggregations and Grouping

- Implement typed `group_by(...)` source DSL.
- Implement aggregation expression builders for count, sum, min, max, avg, and distinct count where practical.
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
- Emit actionable diagnostics that suggest `@special(type="expr")` or hooks when a helper cannot remain compiler-visible.
- Lower higher-order helper plans through shared PySpark recipes.
- Add online/generated parity tests for arrays, maps, nullable elements, and unsupported callback diagnostics.

### Epic: Analytical Joins

- + Implement semi `exists(...)` and anti `not_exists(...)` predicates.
- + Implement `inner_join(...)` for row-multiplying joins.
- + Implement deterministic `JoinDedupe.latest_by(...)` and `JoinDedupe.earliest_by(...)` policies.
- + Implement temporal validity-window `temporal_one(...)` joins for SCD-style lookups.
- + Implement backward `as_of_one(...)` joins with optional tolerance.
- Add tie and overlap policy diagnostics.
- + Show analytical join cardinality in traceability and `structure explain`.
- Add online/generated parity tests for duplicate right rows, unmatched rows, temporal overlaps, and as-of ties.

### Epic: Full PySpark Joins

- Implement `rowset_join(...)` for broad rowset joins.
- Implement right and full outer joins with nullable-side type checks.
- Implement explicit cross joins with `allow_cartesian=True`.
- Implement non-equi join predicates.
- Implement disjunctive join predicates.
- Add backend capabilities for rowset joins and broad predicate classes.
- Reject current-row base constructors after row-admitting joins.
- Show rowset join cardinality, predicate class, nullable sides, and strategy in traceability and `structure explain`.
- Add online/generated parity tests for right-only, left-only, matched, Cartesian, non-equi, and disjunctive cases.

### Epic: Explicit Optimization Directives

- + Implement cache and persist directives at step-method boundaries.
- Implement repartition and coalesce directives.
- Implement checkpoint hints where supported by the configured backend.
- Implement join strategy directives for broadcast, shuffle hash, sort merge, and lookup projection where supported.
- Keep supported directives explicit in source, IR, generated code, and explain output.
- Add diagnostics when a directive is unsafe, unsupported, or likely ignored by the configured PySpark target.
- Add tests proving directives do not change row or schema semantics.

### Epic: Transform Composition Maturity

- Design and implement hook-bearing stages in `.to(...)` composition.
- Define hook method ownership and dispatch for composed runtime pipelines and generated wrapper classes.
- Preserve online/generated parity for composed hooks, including source hook imports and traceability boundaries.
- Decide whether composed transforms can expose selected earlier-stage outputs in addition to final-stage outputs.
- Design whether class-field pipelines may be mixed with wrapper-local step methods, hooks, or lifecycle policy.
- Keep `lane(...)` unavailable for composition matching unless a later design explicitly changes the public boundary.

### Epic: Explain, Documentation, and Test Tooling

- Add rich `structure explain` mode for field-level lineage through projections, filters, joins, aggregations, windows,
  hooks, and optimization boundaries.
- + Add generated documentation artifacts for schemas and transforms in Markdown or JSON.
- + Add pytest helpers for `structure check`, generated-code freshness, generated-code snapshots, expected diagnostics,
  and online/generated parity.

## v.3 Backlog

- Design a unified, minimal decorator parameter vocabulary for `@step` and `@raw`, including binding, output, target,
  schema, and streaming options.
- Defer multi-case `when`, conditional pipeline branches, pattern matching, and transition verbs to a dedicated DSL
  design task.

### Epic: DSL and SQL Function PySpark Parity

- Implement membership predicates.
- Implement range predicates.
- Implement string predicates.
- Implement collection indexing and struct field helpers.
- Implement rich casts.
- Implement ordering modifiers and null ordering descriptors.
- Implement planned string SQL helpers.
- Implement planned date/time helpers.
- Implement planned numeric/math helpers.
- Implement planned predicate helper functions.
- Keep raw SQL strings, raw aliases, raw `Column.over(...)`, UDF/UDTF helpers, and arbitrary Python behavior outside
  the compiler-visible DSL.

### Epic: Join PySpark Parity Hardening

- Implement using-key joins for one key and multiple keys.
- Harden right and full join nullable-side diagnostics.
- Require explicit Cartesian acknowledgement for cross joins.
- Implement supported join strategy directives beyond broadcast.
- Implement forward as-of joins.
- Keep automatic join reordering, nearest as-of joins, lateral joins, table-valued-function joins, stream-stream joins,
  and raw SQL predicates deferred.

### Epic: Aggregation PySpark Parity

- Implement explicit grouping sets.
- Implement post-aggregate `having(...)`.
- Distinguish pre-aggregate filters, metric-local filters, and post-aggregate predicates in docs and diagnostics.
- Keep PySpark dict/list aggregate syntax unsupported.

### Epic: Window PySpark Parity

- Implement null ordering in window order keys.
- Normalize multiple order keys across all window helpers.
- Implement aggregate windows for admitted aggregate helpers.
- Keep raw PySpark `WindowSpec` unsupported.

### Epic: Higher-Order and Collection Helper PySpark Parity

- Implement collection size, array membership, and map-key membership helpers.
- Implement array construction, repeat, union, and except helpers.
- Implement element lookup, safe element lookup, and map concatenation helpers.
- Document missing-key nullability, out-of-range array-index behavior, and duplicate-key behavior.
- Keep row-expanding generator helpers and arbitrary Python callback control flow deferred.

### Epic: Streaming Orchestration

- Add streaming source declarations.
- Add streaming sink declarations.
- Generate `readStream`.
- Generate `writeStream`.
- Add trigger configuration.
- Add checkpoint configuration.
- Add output mode configuration.
- Add watermarks.
- Add admitted stateful streaming policies.
- Add live streaming lifecycle integration evidence.

### Epic: End-of-v.3 Incremental Compile and Cache Diagnostics

- Implement production incremental compilation with `compile --changed-only`.
- Add cache invalidation policies and cache diagnostics for source, config, schema, dependency, generated-target,
  target-profile, and v.3 lifecycle-policy changes.
- Add performance tests for incremental compile on synthetic 10-transform and 100-transform projects.

## Sprint 09 Backlog

- + Promote Spark Connect from experimental parity to supported batch status for completed v.1/v.2 features.
- + Add live online and generated Spark Connect parity tests for the supported batch matrix.
- + Add generated-source scans and backend diagnostics that reject classic-only internals.
- + Add Spark Connect runtime verification through CI or a documented manual script.
- + Document hook, StructureTools, streaming, and storage-write exclusions.
- + Implement static first-slice Spark streaming compatibility for caller-owned streaming DataFrames.
- + Keep generated streaming sources, generated sinks, lifecycle ownership, and query policy deferred behind explicit
  public references.

## Post-Sprint 09 Follow-Up

- Add live online and generated streaming evidence for row-local projection, row-local filtering, schema-only
  validation, and stream-static left/inner lookup joins.
- Implement repartition and coalesce directives.
- Implement checkpoint hints where supported by the configured backend.
- Implement broader join strategy directives for shuffle hash, sort merge, and lookup projection where supported.
- Add rich `structure explain` field-level lineage through projections, filters, joins, aggregations, windows, hooks,
  and optimization boundaries.

## v.4 Backlog

- Continue Spark Connect hardening only for non-batch or explicitly deferred Sprint 09 gaps.
- Explore additional backend families after the PySpark-family batch contract is stable, starting with postponed Polars
  LazyFrame and DuckDB candidates.

## Future Backlog

- Plan partial nested struct updates, such as Structure-native `withField` and `dropFields` equivalents, after nested
  struct construction and whole-field copying are stable.
- Evaluate Ibis as a meta-backend after direct non-PySpark backend candidates clarify the adapter contract.

## Nice To Have Beyond v.4

- Runtime LDJSON traceability emitter. See [NiceToHave.md](NiceToHave.md).
