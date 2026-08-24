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
PySpark transformation coverage. v5 pivots target ownership to a public plugin architecture: Core retains
every workflow engine by default, while bundled and external plugins supply target-specific Plugin API service facets.
A selected
plugin may privately replace a compatible individual engine for its own target; this is not a public extension API.

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
[Challenges.md](../design/Challenges.design.md).

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
  analytical coverage from [AdvancedAnalyticalOperations.md](../specifications/AdvancedAnalyticalOperations.spec.md).
- **Analytical joins:** existence predicates, `inner_join(...)`, deterministic lookup dedupe, temporal validity-window
  joins, and backward as-of joins from [AnalyticalJoinCoverage.md](../specifications/AnalyticalJoinCoverage.spec.md).
- **Full PySpark rowset joins:** right joins, full joins, cross joins, non-equi predicates, and disjunctive predicates
  from [FullPySparkJoinSupport.md](../specifications/FullPySparkJoinSupport.spec.md).
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
management, and CLI orchestration. Plugin plugins participate through one discovered plugin and one negotiated
`PluginAPI` façade, at the highest version supported by Core and that plugin.

### v5 sequence (complete)

- +Sprint 19: specify and implement discovery, plugin manifests, API negotiation, target resolution, Plugin API
  contracts, and Core-owned artifact envelopes.
- +Sprint 20: refactor PySpark authoring, schema, compiler, execution, generation, and capability behavior behind the
  public Plugin API while preserving current runtime and generated-code evidence.
- +Sprint 21: publish the external plugin author contract and conformance kit; build the internal finite-iterable wheel
  to prove real package discovery, execution, serialization, disabling, and isolation.
- +Sprint 22: remove legacy PySpark root exports and backend-specific Core paths, migrate documentation and fixtures,
  run compatibility evidence, and harden v5 for release.

### v5 must include

- Exactly one target per transform or composed pipeline; different transforms in one project may use different
  installed targets.
- Metadata-only package entry-point discovery, with all installed distributions eligible by default and explicit
  distribution disabling for conflicts.
- Short plugin names with deterministic duplicate-plugin diagnostics.
- Explicit minimum/maximum Plugin API versions and highest-mutual-version negotiation in both downgrade directions.
- Core-owned workflows with schema, compiler, capability, execution, generation, and serialization service facets on
  one Plugin API façade.
- Target-owned public field definitions and plugin DSL; no permanent root aliases for PySpark APIs.
- A generic `StructureSession(runtime=..., context=...)` runtime boundary.
- Standard Core diagnostic and capability records whose target-specific content is supplied by plugins.
- A bundled `structure.plugin.pyspark` implementation with no private integration path.
- Supported external plugin development through vendor-owned packages, public documentation, and conformance tests.
- An internal finite-iterable plugin supporting projection, inner/left joins, grouped sum/count, re-iterable results,
  `collect()`, and opaque-plan serialization.

### v5 non-goals

- Source compatibility between target-specific transforms.
- More than one target in a transform or composed pipeline.
- Cross-plugin data exchange, pipeline handoff, or automatic API translation.
- Arbitrary plugin-defined Core workflows.
- Production support or public end-user documentation for the finite-iterable conformance plugin.
- New PySpark transformation families unrelated to completing the plugin extraction.

## v6 Scope

v6 turns the remaining high-value PySpark transformation gaps into a small, explicit, compiler-visible API program.
It first decomposes the oversized PySpark plugin modules, then adds only the typed operations that remove raw hooks in
the shipped Security and Search examples. Every admitted operation has a schema/cardinality contract, diagnostics,
capability classification, traceability, readable generated PySpark, and online/generated parity. Hooks remain the
honest boundary for actions, driver algorithms, arbitrary Python, sources/sinks, and APIs without a settled contract.

### v6 sequence (active)

- Sprint 23 (+): published the v6 API ledger, checked Gaps register, raw-hook migration fixtures, and initial focused
  PySpark delegates without
  feature changes.
- Sprint 24 (+): admitted lambda-bound struct field access, explicit analytic maxima, deterministic ordered
  collections, global aggregates, and `exactly_one` relation-cardinality assertions; replaced both Security raw hooks
  with ordinary steps and documented the scalar UDF boundary. Repository gate passed on 2026-07-26.
- Sprint 25 (+): admitted typed relation operations—generators, branch/set composition, self aliases, ordering, bounds,
  assertions including parent references, bounded hierarchy/fallback expansion, and priority selection—and retired the
  scheduled Search hooks in independently verifiable slices.
- Sprint 26 (+): delivered the separately specified bounded ordered timeline `scan(...)` recurrence feature using
  the stable relation/recipe boundaries from Sprint 25.
- Sprint 27 (active): close release evidence and the unaddressed Challenges.md inventory, with no new feature family
  admitted.

### v6 must include

- A checked API ledger classifying every remaining catalog entry as implemented, scheduled, deferred, or intentionally
  unsupported, with one source of truth for documentation, capability reporting, and example hooks.
- Behavior-preserving extraction of the large PySpark DSL, symbolic-execution, compiler-traceability, online-runner,
  and generated-rendering classes into focused delegates.
- Lambda-bound struct field access and explicit window aggregate helpers sufficient to make Security reconciliation
  compiler-visible and remove both of its raw hooks.
- Deterministic ordered aggregate collection, exactly-one validation, and global aggregate semantics including empty
  input behavior while preserving aggregate-only methods without a preceding `group_by(...)` call.
- A documented, shipped opt-in scalar `@special(type="udf")` example for ordinary PySpark, including declared
  return/nullability contracts and warning guidance; this is not automatic UDF fallback and remains excluded from
  Spark Connect.
- Typed relation operations with declared schemas and cardinality for the Search migrations: row generators,
  branch/union composition, self aliases, ordering, limit, offset, relation assertions including parent references,
  bounded hierarchy closure/fallback expansion, and declared-key priority selection.
- The bounded batch-only `scan(...)` recurrence feature from
  `close/archive/planning/P07182601.V6-timeline-scan-recurrence.plan.md`, including partitioned Fibonacci evidence.
- A maintained disposition for Challenges C27--C34, executable specification coverage for each new feature, and
  runnable operational/adoption recipes where the challenge calls for documentation.

### v6 non-goals

- A general PySpark wrapper; raw `Column`, `DataFrame`, `WindowSpec`, SQL strings, UDTF, RDD, Pandas, and action APIs
  remain outside the typed DSL. Explicit scalar `@special(type="udf")` remains the existing opt-in exception, never a
  compiler fallback.
- Binary/encoding helpers without a public Binary field type, JSON/CSV parsing without inline-schema/option contracts,
  and deterministic grouped `mode(...)` were v6 non-goals that v7 delivered through typed contracts.
- Relation sampling, repartition/coalesce/checkpoint, and broader physical-plan directives without their own
  performance and reproducibility contracts.
- Input-less transforms, generated source frames, persistent recurrence state, global/unbounded scans, and all
  streaming scans.
- Replacing the School matrix-inversion hook, which deliberately materializes rows and runs a Python numerical
  algorithm rather than a DataFrame transformation.

## v7 Scope

v7 broadens the typed PySpark transformation API after v6's hook-retirement program and advances caller-owned
streaming adoption only where state, output, and lifecycle semantics are explicit. The release remains a
compiler-visible transformation library: it does not become a general PySpark wrapper or a streaming-job owner.

### v7 sequence

- Sprint 28 complete: created the checked PySpark 3.5.x/4.0.x coverage catalog, reconciled v4--v6 deferrals, delivered
  the first v7 typed coverage slices, admitted raw-hook-bearing composition, and moved Search labeling onto that generic
  path.
- Sprint 29 complete: completed focused generator delegate extraction and typed struct-generator expansion.
- Sprints 30--32 complete: added Binary encoding, Schema-carrying JSON/CSV conversion, and PySpark-named grouped
  `mode(...)` with portable deterministic tie lowering.
- Sprint 33 complete: admitted stream-static inner, left, and left-semi enrichment with caller-owned restart evidence.
- Sprint 34 complete: hardened stream-static left-outer lookup with nullable lookup projection diagnostics and
  online/generated restart evidence on PySpark 3.5 and 4.0.
- Sprint 35 complete: admitted one already admitted stateful operation followed only by stateless transforms, with
  target-matrix restart evidence and caller lifecycle ownership preserved.
- V7 closeout complete: the coverage and staged caller-owned streaming adoption work is wrapped; percentage-based
  Structured Streaming parity moves to v8.

### v7 must include

- One authoritative coverage catalog whose entries state user value, schema/cardinality effect, batch and streaming
  eligibility, capability, diagnostics, traceability, generated form, target evidence, and disposition.
- Broad typed transformation coverage across the high-value PySpark DataFrame and Column families selected by the
  catalog, preserving online/generated parity and readable generated code.
- Completion of the deferred v6 focused-delegate work for oversized operation, expression, scope, result, evaluation,
  execution, rendering, and traceability components.
- Admission of Binary encoding, Schema-carrying JSON/CSV conversion, and deterministic mode with typed contracts;
  untyped/raw forms remain outside the DSL.
- Staged caller-owned streaming support—stream-static enrichment, left-outer lookup, and one-stateful-plus-stateless
  composition—with caller-retained sources, sinks, checkpoints, triggers, output-mode calls, and query lifecycle.

### v7 non-goals

- Raw DataFrame/Column APIs, actions, loading/storage, catalog management, raw SQL, RDD/Pandas, arbitrary Python
  callbacks, untyped UDTFs, and automatic UDF fallback.
- Structure-owned streaming sources, sinks, checkpoints, triggers, output modes, query lifecycle, or side effects.
- Unbounded or chained stateful streaming, Spark Connect streaming, and physical-plan controls unless a separately
  approved design changes the scope.
- Production incremental compilation/cache diagnostics, general external-plugin expansion, data-quality constraints,
  and Search-only evaluation follow-ups; these stay explicitly retained backlog.

## v8 Scope

v8 focuses on PySpark Structured Streaming coverage parity. It does not broaden Structure into a streaming job
orchestrator. The release measures caller-owned streaming transformation support against the same checked PySpark
transformation catalog used for batch coverage, then closes the percentage gap with honest operation-level accounting
where Spark supports only part of a batch family.

### v8 sequence (complete)

- Sprint 36 complete: published the checked Structured Streaming coverage ledger and guard tests.
- Sprint 37 complete: admitted typed struct generators and exact-schema stream-stream union-like set operations, and
  closed ordering and priority selection as explicit streaming-ineligible rows with targeted live PySpark 3.5/4.0
  restart evidence.
- Sprint 38 complete locally: resolved stateful and order-sensitive gaps with explicit streaming-ineligible
  diagnostics.
- Sprint 39 complete: hardened v8 release evidence without adding new feature scope.

### v8 must include

- A measured streaming coverage percentage that is at least the current batch coverage percentage under the checked v8
  rule.
- Operation-level ledger rows for mixed families such as set operations and ordering.
- Live PySpark 3.5 and 4.0 file-stream restart evidence for every admitted streaming operation.
- Generated-source scans proving Structure emits no streaming lifecycle, action, RDD, Pandas, or hidden UDF calls in
  streaming-compatible generated transforms.
- Corrective diagnostics for Spark-ineligible streaming shapes.

### v8 non-goals

- Generated streaming sources, sinks, checkpoints, triggers, output modes, query names, start/stop behavior, deployment,
  or recovery.
- Spark Connect streaming.
- Two-stateful chains unless a later explicit Spark-supported contract changes this scope.
- Arbitrary global ordering, limits, offsets, or selected-row ranking over unbounded streaming inputs.

## v9 Scope

v9 focuses on broad PySpark streaming API coverage and adoption after v8 closed transformation percentage parity. The
release creates a checked Structured Streaming API ledger that covers more than transformation families: input modes,
watermarks, stateful transformations, DataStreamReader, DataStreamWriter, output modes, triggers, checkpoints, query
lifecycle, side-effect APIs, listener/progress APIs, arbitrary state APIs, and Spark Connect streaming. The ledger must
separate Structure-owned typed transformations from caller-owned PySpark lifecycle code.

### v9 sequence

- Sprint 40 complete: published the checked PySpark streaming API ledger and reconciled streaming-relevant v7/v8 deferrals.
- Sprint 41 complete: published and tested caller-owned adoption recipes for sources, sinks, checkpoints, triggers, output modes,
  query lifecycle, and generated Structure transforms.
- Sprint 42 complete: re-evaluated stateful and order-sensitive streaming API gaps, admitting only shapes with explicit state,
  watermark, output-mode, diagnostics, and live restart evidence.
- Sprint 43 complete: hardened lifecycle diagnostics, explain output, troubleshooting, and owner-boundary documentation.
- Sprint 44 complete: closed v9 release evidence without adding new API scope.
- Sprint 45 complete (closed 2026-08-02): inventoried and closed the existing V9 design-gate decisions.
- Sprint 46 complete (closed 2026-08-02): completed finite selected-value support, broad selected-row and analytic-window
  decisions, the Variant child plan, arbitrary-state contract, and provider-neutral Geometry contract.
- Sprint 47 complete (closed 2026-08-02): collected pinned PySpark 3.5/4.0 evidence and reconciled the API catalog,
  ledgers, diagnostics, generated artifacts, and public documentation; the unavailable 4.2 lane is recorded honestly.
- Sprint 48 complete (closed 2026-08-02): performed dedicated V9 hardening with no new API scope, final `make build`,
  and the release evidence report.
- The governing closeout schedule is
  `docs/dev/planning/P07302603.V9-closeout-and-release.plan.md`; XML remains low-priority design-gated work.

### v9 must include

- A checked PySpark streaming API ledger with status, owner boundary, evidence path, and support-claim accounting for
  each selected API family.
- Runnable caller-owned adoption examples showing Structure inside a real Structured Streaming application without
  hidden lifecycle generation.
- Reconciliation of deferred streaming-related items from v7 and v8 into implemented support, explicit ineligibility,
  design-gated backlog, or caller-owned guidance.
- Corrective diagnostics and explain output that name whether the fix belongs in Structure source, caller-owned PySpark
  lifecycle code, or a batch materialization boundary.
- Live PySpark 3.5 and 4.0 evidence for every admitted Structure-owned streaming claim, plus `make build` in the final
  hardening sprint.
- A follow-up execution plan for design-gated catalog rows:
  `docs/dev/planning/P07302601.V9-api-catalog-design-gates.plan.md`, including the Variant child plan
  `docs/dev/planning/P07302602.V9-variant-type-and-helpers.plan.md`.
- The released PySpark 4.0/4.2 Variant implementation slice is wrapped up; 4.3+ mutation helpers stay design-gated
  until their profiles are released, with a live 4.2 probe tracked as infrastructure follow-up.

### v9 non-goals

- Making Structure a default streaming job orchestrator.
- Hidden generation of sources, sinks, triggers, checkpoints, output modes, starts, stops, deployment, or recovery.
- Side-effect ownership through `foreach` or `foreachBatch` without a separate idempotence and recovery design.
- Spark Connect streaming promotion without a separate target contract and live evidence.
- Non-streaming retained backlog from v7, including Search evaluation follow-ups, plugin-owned DSL completion,
  data-quality constraints, and incremental compile cache diagnostics, unless a selected v9 streaming slice directly
  needs it.

## v10 Scope

V10 follows the completed V9 design-gate closeout and expands only the core API and streaming contracts explicitly
adopted from `docs/dev/future/API.future.md` and `docs/dev/future/Streaming.future.md`. It keeps streaming lifecycle,
deployment, recovery, and side effects caller-owned.

### v10 closeout status

The environment-independent V10 scope is conditionally complete as of 2026-08-22. Catalog, ledger, diagnostic,
documentation, generated-artifact, collision-safety, and package/build reconciliation is complete. Docker-dependent
live target evidence remains unavailable, and SearchDocuments streaming remains design-gated; see
[V10 Release Evidence](V10ReleaseEvidence.md).

### v10 sequence

- Sprint 49 (2026-09-21--2026-10-02): V10 admission, V9 handoff, and grouped ExecPlan foundation.
- Sprint 50 (2026-10-05--2026-10-16): API Catalog contracts, Geometry, sampling, and open-row dispositions.
- Sprint 51 (2026-10-19--2026-10-30): schema evolution and missing-column union evidence.
- Sprint 52 (2026-11-02--2026-11-13): compiler-visible streaming state stages and stream-stream join contracts.
- Sprint 53 (2026-11-16--2026-11-27): caller-owned side-effect safety and arbitrary-state programming model.
- Sprint 54 (2026-11-30--2026-12-11): dedicated V10 hardening and release evidence.

The governing V10 plans are the four `P08022601`--`P08022604` documents under `docs/dev/planning/`, together with the
Sprint 54 hardening plan `P08042601.Collision-safe-generated-identities.plan.md` and the explicitly adopted Search
application proving slices `P08052602.Search-vector-index-and-rrf.plan.md` and
`P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md`. Other application-specific future
documents remain outside V10.

### v10 must include

- Provider-neutral Geometry, explicit sampling reproducibility, and typed schema-evolution decisions.
- `union_by_name(..., defaults=...)` contract work for nullable, nested-struct, alias-preserving evolution, with
  streaming support claimed only after target evidence.
- State-stage metadata, bounded stream-stream join candidates, finite selected-row alternatives, and corrective
  diagnostics for unsafe state composition.
- Caller-owned idempotence/recovery guidance for streaming side effects and an implementation-ready arbitrary-state
  model without Structure-owned lifecycle generation.
- Synchronized API Catalog, capability inventories, diagnostics, references, examples, generated artifacts, live target
  evidence, and final `make build`.
- Search vector-index/RRF work is complete to the bounded plan contract, with caller-owned embedding production,
  generated/online parity, judged-search evidence, and no accidental adoption of the broader Search future inventory.

### v10 non-goals

- Structure-owned streaming sources, sinks, triggers, checkpoints, output modes, query lifecycle, deployment, recovery,
  `foreach`, `foreachBatch`, custom sinks, or external side effects.
- Silent support claims for XML, unreleased Variant mutation profiles, unsafe join reordering, arbitrary state, or
  unsupported stream-stream joins.
- Search futures outside `P08052602.Search-vector-index-and-rrf.plan.md`, including model execution, external ANN
  services, answer generation, adaptive chunking, and streaming vector-index maintenance. The scalar-generator/Search
  chunking proving slice is explicitly limited to `P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md`.
- Store, Stocks, Streams, or School application futures.

## Release Milestones

| Milestone | Goal | Sprints |
|---|---|---|
| M0 | Repository, compiler skeleton, and pre-coding spike gate | Sprint 00 |
| M1 | first executable slice | Sprint 01 |
| M2 | Schema validation and generated class polish | Sprint 02 |
| M3 | Practical expression DSL and diagnostics | Sprint 03 |
| M4 | Hook model and no-hook generated-code cleanliness | Sprint 04 |
| M5 | Joins, compiler traceability, build integration | Sprint 05 |
| M6 | v1 stabilization and example background docs | follow-up hardening sprint |
| M7 | v2 analytical pipeline features, analytical join coverage, composition maturity, adoption tooling, and Spark Connect batch support | Sprints 06-09 |
| M8 | v3 PySpark gap closure and streaming transformation hardening | Sprints 11-16 |
| M9 | v4 PySpark transformation API coverage | Sprint 17, later v4 feature sprints including Sprint 18 streaming migration, then the final v4 hardening sprint |
| M10 | v5 Core-orchestrated plugin architecture | Sprints 19-22 |
| M11 | v6 typed PySpark API closure, example-hook retirement, and bounded recurrence | Sprints 23-27 |
| M12 | v7 broad PySpark transformation coverage and caller-owned streaming adoption | Sprints 28--35 |
| M13 | v8 PySpark Structured Streaming coverage parity | Sprints 36--39 and v8 hardening |
| M14 | v9 PySpark streaming API coverage, design-gate follow-up, and release hardening | Sprints 40--48 |
| M15 | v10 API Catalog and streaming contract expansion | Conditionally complete 2026-08-22; Sprints 49--54 evidence retained |
