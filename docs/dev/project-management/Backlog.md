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

## v3 Completed Scope

All scheduled v3 implementation items below are complete. This retained pre-delivery checklist records the release
boundary; unimplemented parity work is now marked `planned` in `docs/dev/Gaps.md` and will receive separate plans.

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

### Epic: Streaming Transformation Hardening

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

### Epic: V4 Transformation API Coverage

- Create a checked catalog for relevant PySpark 3.5.x/4.0.x transformation APIs.
- Close high-value Column and SQL-function gaps with typed symbolic contracts.
- Add nested struct/collection and declared-parser coverage without type erasure.
- Add schema-aware relational transformations and planned analytical gaps.
- Admit generators only after a schema-and-cardinality design gate.

## V10 Backlog: API Catalog and Streaming Contract Expansion

### Epic: API Catalog and Schema Evolution

- Define and verify provider-neutral Geometry with positive literal SRIDs, WKT operations, nullable predicates, and
  optional-provider diagnostics.
- Preserve sampling's explicit seed/reproducibility policy and batch-only streaming boundary.
- Implement or explicitly gate `union_by_name(..., defaults=...)` for nullable, nested-struct, alias-preserving schema
  evolution; reject implicit array/map element evolution.
- Reconcile XML, Variant mutation profiles, and opt-in join reordering with precise catalog statuses.

### Epic: Streaming State and Join Contracts

- Record ordered state-stage metadata for aggregates, dedupe, windows, and stream-stream joins.
- Prototype bounded cross and anti stream-stream candidates with watermarks, event-time bounds, retention, and output
  mode requirements.
- Preserve finite grouped selected-value alternatives and explicit batch boundaries for global selected-row and broad
  analytic-window helpers.

### Epic: Side-Effect Safety and Arbitrary State

- Document caller-owned sink identity, idempotence, retry, checkpoint, failure, recovery, and callback security rules.
- Test caller-owned `foreachBatch` adoption and generated-source lifecycle cleanliness.
- Specify typed arbitrary-state input/state/output schemas, timeout clocks, initialization, cleanup, target profiles,
  generated-code boundaries, hooks, and restart evidence.

### Epic: Evidence and Hardening

- Keep API Catalog, capability ledgers, diagnostics, references, examples, generated artifacts, and background
  companions synchronized.
- Preserve full source identity in every generated backend module, schema symbol, schema document, traceability path,
  and plugin-generated file map; reject conflicting duplicate paths instead of overwriting them.
- Run PySpark 3.5/4.0 live parity and restart lanes for every claimed streaming support row.
- Run optional-provider evidence only with pinned dependencies and finish Sprint 54 with `make build`.

The V10 backlog is governed by `docs/dev/project-management/V10.md`, the grouped plans P08022601–P08022604, and the
Sprint 54 hardening plan P08042601. The explicitly adopted Search application proving slice is governed by
P08052602.Search-vector-index-and-rrf.plan.md; other application-specific future documents are not V10 backlog
commitments.

### Epic: Search Vector Index and Reciprocal Rank Fusion

- Add caller-supplied document and paragraph embedding contracts with model, dimension, content-revision, and
  experiment identity validation.
- Build an exact typed vector index and cosine top-K scorer without model invocation, driver-side collection, or an
  external ANN dependency.
- Refactor document and paragraph similarity to union lexical/vector ranked candidates and apply equal-weight RRF.
- Extend document search with vector candidates before feedback reranking while preserving lexical-only compatibility
  and the caller-owned streaming lifecycle boundary.
- Add generated/online parity, judged-ranking comparisons, documentation, and v10 release evidence.

## Sprint 09 Backlog

- + Promote Spark Connect from experimental parity to supported batch status for completed v1/v2 features.
- + Add live online and generated Spark Connect parity tests for the supported batch matrix.
- + Add generated-source scans and backend diagnostics that reject classic-only internals.
- + Add Spark Connect runtime verification through CI or a documented manual script.
- + Document hook, StructureTools, streaming, and storage-write exclusions.
- + Implement static first-slice Spark streaming compatibility for caller-owned streaming DataFrames.
- + Keep generated streaming sources, generated sinks, lifecycle ownership, and query policy permanently caller-owned
  behind explicit public references.

## Future Example Apps

- Add a batch-only telemetry/time-series example for custom windows, ranking, lag/lead, rolling metrics, and published
  `RaceWinner` results. Keep it separate from the streaming `examples/streams` contract.

## Post-Sprint 09 Follow-Up

- Add live online and generated streaming evidence for row-local projection, row-local filtering, schema-only
  validation, and stream-static left/inner lookup joins.
- Implement repartition and coalesce directives.
- Implement checkpoint hints where supported by the configured backend.
- Implement broader join strategy directives for shuffle hash, sort merge, and lookup projection where supported.
- Add rich `structure explain` field-level lineage through projections, filters, joins, aggregations, windows, hooks,
  and optimization boundaries.

## v4 Backlog

- Maintain one classification for every PySpark transformation API in the supported target range so missing parity is
  visible and actionable.
- Keep loading, storage, actions, orchestration, and alternative backends out of v4.
- Deliver Sprint 18 caller-owned streaming migration: static-gap session aggregation; bounded stream-stream
  left/right/full outer and left-semi joins; and stream-static semi filtering.
- Require compiler-visible watermarks, event-time bounds, input modes, and caller-required output modes for every
  stateful streaming shape; retain dynamic session gaps, chained stateful plans, unbounded state, and lifecycle APIs
  as exclusions.
- Add generated/online parity, explain, diagnostics, and live PySpark 3.5.x/4.0.x evidence before marking an
  admitted streaming family supported.

## Future Backlog

- Implement production incremental compilation and cache diagnostics after the transformation coverage program and a
  dedicated reprioritization decision. The existing ExecPlan remains a design input, not a version commitment.
- Plan partial nested struct updates, such as Structure-native `withField` and `dropFields` equivalents, after nested
  struct construction and whole-field copying are stable.
- Evaluate Ibis as a meta-backend after direct non-PySpark backend candidates clarify the adapter contract.

## Nice To Have Beyond v4

- Runtime LDJSON traceability emitter. See [NiceToHave.md](NiceToHave.md).

## v6 Backlog

### Epic: API Ledger and Plugin Decomposition

- Publish a single v6 PySpark API ledger linking catalog status, contract, capability, diagnostics, examples, and test
  evidence; update `docs/dev/Gaps.md` with every postponed/deferred disposition.
- Add a raw-hook inventory test for all shipped examples and require an explicit retirement/defer/intentional status.
- Extract focused delegates from `operations_api`, expression construction, `InputScope`, result-body building,
  expression evaluation, online execution, step rendering, transform-module rendering, and compiler traceability.
- Preserve wildcard public imports, rendered output, recipes, online behavior, and app-boundary rules during each
  behavior-preserving extraction.

### Epic: Compiler-Visible Security Reconciliation

- Implement typed lambda-bound struct field access for collection callbacks.
- Implement explicit partitioned analytic maximum helpers.
- Implement deterministic ordered collection and exactly-one validation contracts.
- Preserve aggregate-only methods without a preceding `group_by(...)` call and document their global aggregate
  empty-input result rule.
- Add a shipped, documented opt-in scalar `@special(type="udf")` example with return type, nullability, and
  `warn_on_udfs` guidance; keep it separate from unsupported-operation fallback.
- Replace Security reconciliation raw hooks with steps and remove placeholder output fields.
- Add symbolic, recipe, generated-source, online/generated parity, live PySpark, traceability, and hook-inventory
  evidence for each operation.

### Epic: Typed Relation Operations and Search Migration

- Design schema/cardinality contracts for `posexplode` first, then the remaining admitted generator forms.
- Implement typed `union_all`/`union_by_name` before set operations with distinct duplicate semantics.
- Implement self-alias relation scopes for explicit self joins.
- Implement typed relation `order_by`, literal `limit`, and literal `offset`; defer `sample` pending a reproducibility
  contract.
- Implement branchable union, relation assertions including parent references, bounded parent-hierarchy closure and
  deterministic fallback expansion, and declared-key
  first-qualified priority selection.
- Migrate `Chunking`, overlap/BM25 scoring, index summaries, similarity queries, score reduction, relevance-context
  expansion, document reranking, and cohort-band resolution only after their prerequisite operations are proven.
- Compare normalized, deterministic rows from the raw and typed implementations before removing every hook.

### Epic: V11 PySpark 4.1 Adoption

- Inventory every reviewed PySpark 4.1 Python API addition against the 4.0 baseline and give it exactly one Structure
  status, owner boundary, capability key, diagnostic, test, and evidence path.
- Add the exact `>=4.1,<4.2` ordinary and Spark Connect capability profiles while preserving 3.5/4.0 regression lanes.
- Implement the approved typed 4.1 expression, `Column.transform`, existence/IN-subquery, and lateral-relation slices.
- Decide and specify complex observations and KLL/Theta sketch aggregates; retain explicit gates where metric or binary
  semantics are not typed and deterministic.
- Document and test caller-owned boundaries for Arrow UDF/UDTFs, row-based `transformWithState`, Declarative Pipelines,
  SQL Scripting, Python Data Sources, readers, writers, sessions, and catalogs.
- Extend Compose, runner selection, backend metadata, fixtures, version assertions, CI, and evidence reports to
  `pyspark41` and `spark-connect41`.
- Reconcile API Catalog, API Reference, machine-readable inventories, compatibility/troubleshooting docs, generated
  artifacts, traceability, and release evidence before deciding whether the default profile includes 4.1.

### Epic: Bounded Timeline Recurrence

- Implement the separately planned typed, ordered, bounded `scan(...)` feature and Fibonacci evidence.
- Keep global/unbounded/streaming scans and synthetic source frames explicitly unsupported.

### Epic: Challenge and Release Closure

- Audit C27 analytical-join status against the current implementation and correct stale challenge wording.
- Add C28 operational recipes and troubleshooting links for local, CI, generated-artifact review, packaged-wheel, and
  one managed-Spark deployment path.
- Add C30 executable-specification coverage for every v6 feature.
- Prepare the C31 licensing/governance decision record and publication checklist for project-owner approval.
- Verify or schedule C32 field-alias correctness for Python keywords/nonidentifiers.
- Specify C33 hook ownership for composed transforms before enabling hook-bearing composition.
- Replace C34's prose hook list with the maintained API ledger and raw-hook inventory.
