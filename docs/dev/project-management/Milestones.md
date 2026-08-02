# Milestones

## M0: Groundwork Ready

Status: v1 closeout. Configuration resolution, CLI entrypoint, seed config generation, Spark-free `check`, Spark-free
`compile`, generated-output diff checks, and spike outcomes are implemented and tested. Remaining closeout is CI
workflow evidence before this milestone should be marked with `+`.

### Exit Criteria

- Repository layout exists.
- Python package imports successfully.
- CLI skeleton runs.
- Seed TOML config can be loaded.
- Config resolution order is explicit and covered by tests.
- Invalid config keys and values fail with structured diagnostics.
- Test harness runs in CI.
- Source-root discovery and generated output conventions are settled.
- Online execution default is reflected in seed configuration.
- Python and PySpark compatibility policy is documented and reflected in seed configuration.
- Sprint 00 spike notes are captured for decorators, expression helpers, source order, import paths, no-Spark compile,
  and local Spark execution.
- Any spike result that changes v1 scope is reflected in Sprint 01 before coding begins.
- Architecture docs are linked from project README.

### Demonstration

```bash
structure --help
structure check
pytest
```

## M1: Vertical Slice 1

Status: v1 external validation. The shared PySpark recipe layer, generated transform rendering, public `StructureSession`, deferred
input binding, generated runner delegation, runtime input diagnostics, live online PySpark recipe interpretation, and
online/generated row parity integration coverage are implemented. Remaining closeout is running the PySpark integration
matrix in an environment with PySpark installed, because the local workspace skips those tests.

### Exit Criteria

- A simple schema and transform run online through `StructureSession`.
- The same transform can optionally compile to generated PySpark.
- Generated PySpark class imports successfully.
- Online transform runs in a local Spark test.
- The compiled path uses `select(...)` and `F.col(...)`, not UDFs.
- Generated code is deterministic and formatted.

### Demonstration

```python
NormalizeOrders(orders=orders_df).run(session)
```

## M2: Schema Enforcement

Status: v1 closeout. Spark schema source rendering, generated schema modules, generated runtime schema helpers,
runtime schema materialization, validation recipe placement, and online-materialized `result.schema[output_name]`
exposure are implemented and tested. Live runtime schema assertion behavior is covered through the online/generated
parity integration contract. Remaining closeout is broader negative schema-validation coverage against Spark
DataFrames.

### Exit Criteria

- Spark `StructType` is generated from schemas.
- Primitive, array, map, and nested struct schema fields are supported.
- `assert_schema(...)` validates inputs and outputs.
- Intermediate validation is enabled by default.
- Class-wide and method-level validation overrides work.
- Invalid schema tests fail predictably.

## +M3: Expression DSL Usability

Status: v1 local closeout complete. v1 fixture expressions, filters, expression helpers, generated expression
rendering, literal typing, output assignment checks, nullability narrowing, explicit conversion diagnostics, and the
shared diagnostic registry are implemented and tested. Unsupported Python operation diagnostics and live
online/generated expression parity integration coverage are in place.

### Exit Criteria

- Common expression functions compile.
- `where(...)` filters compile.
- `@special(type="expr")` helpers compile and inline.
- Unsupported Python operations fail with structured diagnostics.
- Error messages suggest DSL alternatives, `@special(type="expr")`, hooks, and config workarounds when applicable.

## +M4: Hooks and Generated Classes

Status: complete. Hook metadata, source hook calls, `HookInputs`, hook schema modes, project-output validation,
hook-free generated cleanliness, streaming compatibility findings, traceability opaque boundaries, and online/generated
hook recipe parity are implemented and tested for v1.

### Exit Criteria

- Online execution is the primary runtime path.
- Generated classes remain optional artifacts.
- Hook-free transforms do not import source transform classes.
- Hooked transforms direct-import source class and call hooks.
- Hook signature is validated.
- `@raw(lane=lane)` and `@raw(lane=lane)` work.

## M5: Joins, Compiler Traceability, Build Integration

Status: v1 external validation. `lookup_join(...)`, source-order join lowering, generated join rendering, uniqueness
warnings, stricter join-condition/key diagnostics, `compile --fail-on-diff`, compact `structure explain`, streaming
compatibility reporting, compiler provenance, static dataflow traceability artifacts, compact explain traceability
summaries, and online/generated join parity integration coverage are implemented and tested. Remaining exit criteria
are broader CI build-integration coverage and validating the live PySpark matrix outside this PySpark-free workspace.

### Exit Criteria

- `lookup_join(...)` compiles to PySpark joins.
- N-step serial joins work across arbitrary named inputs.
- Compiler provenance maps source nodes to IR nodes to generated PySpark nodes.
- Static dataflow traceability shows transform, table, and column dependencies inferred from IR.
- `structure compile --fail-on-diff` works.
- `structure explain` summarizes inputs, steps, filters, joins, hooks, and validation.
- Streaming compatibility reports whether transforms are compatible, batch-only, or unknown.

## M6: v1 Stabilization

Status: v1 closeout. The first registry-backed diagnostic contract is implemented with public anchors, renderer,
registry validation tests, and representative routing for configuration, schema assignment, joins, target capability,
generated-output drift, runtime, CLI internal errors, compiler errors, and streaming compatibility findings. Remaining
stabilization work includes multi-version PySpark execution evidence, generated-code version headers, and setup/doctor
checks.

### Exit Criteria

- Compatibility docs, generated-code version headers, compiler traceability schema versioning, and config schema
  compatibility are checked against release artifacts.
- Multi-version PySpark test strategy covers the documented v1 target range.
- Diagnostic codes link to relevant documentation.
- Setup/configuration doctor checks the common adoption failures.

## M7: v2 Analytical Pipeline Features

Status: v2 wrapped. Sprints 06-10 delivered analytical foundations, analytical joins, aggregation/window/HOF coverage,
Spark Connect batch support, static caller-owned streaming compatibility, generated docs, and pytest helpers. Production
incremental compile and cache diagnostics remain future work.

### M7A: v2 Scope and Analytical IR Foundations

Exit Criteria:

- v2 user stories, backlog epics, milestone split, and sprint charters are published.
- Analytical operation IR records operation kind, input scope, output schema, source location, backend capability,
  cardinality, and streaming compatibility classification.
- Shared PySpark recipe boundaries are ready for aggregation, window, higher-order function, optimization hint, and
  analytical join lowering.
- v2 fixture packages cover small, readable orders-style examples for aggregation, windowing, arrays/maps, and
  analytical joins.
- Diagnostics use stable codes and link to the relevant v2 specification or roadmap section.

### M7B: Analytical Join Coverage

Progress:

- Existence joins, `inner_join(...)`, deterministic deduped `lookup_join(...)`, and temporal validity-window joins are
  implemented for the default PySpark profile.
- Backward as-of joins, analytical join traceability, explain output, and streaming compatibility classification are
  implemented for the default PySpark profile.
- Runtime overlap diagnostics remain open.

Exit Criteria:

- Existence joins cover semi and anti filter semantics without exposing right-side fields.
- `inner_join(...)` has clear row-multiplication semantics and online/generated parity tests.
- Deterministic lookup dedupe policies never rely on arbitrary right-row selection.
- Temporal validity-window joins support SCD-style lookups with explicit overlap policy.
- Backward as-of joins support time-relative enrichment with optional tolerance.
- Traceability and `structure explain` show row-filtering, row-multiplying, select-one, temporal, and as-of cardinality.

### M7C: Aggregations, Windows, and Higher-Order Functions

Progress:

- Sprint 08 completed the first analytical slice: grouped aggregates, selected-row helpers, exact/subset dedupe,
  projection windows, rolling row metrics, and basic array/map higher-order helpers.
- Sprint 09 carried advanced analytical support from
  [P07052601.Advanced-analytical-operations.plan.md](../../../close/archive/planning/P07052601.Advanced-analytical-operations.plan.md).

Exit Criteria:

- Typed `group_by(...)` and aggregation support covers count, sum, min, max, avg, distinct count where practical,
  and schema-checked aggregate output construction.
- Advanced grouping support covers rollup, cube, explicit grouping sets, subtotal metadata, filtered metrics,
  post-aggregate `having(...)`, and additional exact, statistical, approximate, and collection metrics admitted by the
  Sprint 09 and Sprint 13 specifications.
- Windowing covers latest-row, ranking, lag/lead, rolling metrics, and duplicate-removal helpers.
- Broad windowing covers reusable window specs, explicit row/range frames, distribution/value helpers, and aggregate
  window expressions admitted by the Sprint 09 specification.
- Deduplication helpers expose deterministic tie policies and never lower to arbitrary `dropDuplicates(...)` when a
  selected row matters.
- Spark higher-order helpers for arrays and maps remain Spark-plan-visible and reject unsupported Python callbacks.
- Advanced HOF support covers additional array and map helpers while preserving symbolic callback diagnostics.
- Online/generated parity tests cover every admitted aggregation, window, dedupe, and higher-order helper form.

### M7D: Optimization, Explain, Docs, and Test Tooling

Exit Criteria:

- Cache/persist first-slice directives are explicit in source and obvious in generated code.
- Right, full, cross, non-equi, and disjunctive rowset joins are represented in source, IR, backend capability checks,
  generated code, online recipes, traceability, and explain output.
- Repartition, coalesce, checkpoint, and broader join strategy directives are deferred until their physical-plan
  contract is specified.
- Compact explain output shows operation families, cardinality, streaming classification, traceability, and static
  dataflow; richer field-level lineage remains follow-up work.
- Generated documentation artifacts describe schemas, transforms, inputs, outputs, traceability, and generated targets
  in Markdown or JSON.
- Pytest helpers cover `structure check`, generated-code freshness, generated-code snapshots, diagnostics, and
  online/generated parity fixtures.
- Production incremental compile and cache diagnostics remain future work outside M9.

### M7F: Transform Composition Maturity

Exit Criteria:

- Hook-bearing stages in `.to(...)` composition have a specified owner and dispatch model for both online and generated
  execution.
- Composed hook traceability, source imports, lifecycle ordering, and validation boundaries have online/generated parity
  tests.
- The project has an explicit decision on whether composed wrappers may expose earlier-stage outputs, mix local
  step methods or hooks with class-field pipelines, or remain final-output-only composition shells.
- `lane(...)` remains unavailable for composition matching unless a later accepted design changes the public transform
  boundary.

## +M8: v3 PySpark Gap Closure and Streaming Transformation Hardening

Status: complete. Sprints 11-16 delivered the completed v3 PySpark feature surface and caller-owned streaming
transformation hardening.

### M8A: DSL and SQL Function PySpark Parity

Exit Criteria:

- Membership, range, string, indexing, struct field, cast, and ordering Column helpers are compiler-visible.
- Planned string, date/time, numeric, and predicate SQL functions are compiler-visible.
- Online/generated parity, backend capabilities, diagnostics, docs, compatibility tables, explain, and traceability are
  updated.

### M8B: Join PySpark Parity Hardening

Status: complete.

Exit Criteria:

- Using-key joins support one key and multiple keys.
- Right/full diagnostics name nullable sides and invalid output fields.
- Cross joins require explicit Cartesian acknowledgement.
- Supported join strategy directives are capability checked.
- Forward as-of joins have deterministic tolerance and tie behavior.

### + M8C: Aggregation PySpark Parity

Exit Criteria:

- Explicit grouping sets are implemented or capability-gated with diagnostics.
- Post-aggregate `having(...)` has a typed aggregate-output predicate scope.
- Docs and tests distinguish pre-aggregate `where(...)`, metric-local filters, and post-aggregate `having(...)`.

### M8D: Window PySpark Parity

Exit Criteria:

- Window order keys support explicit null ordering.
- Every window helper accepts multiple order keys consistently.
- Aggregate windows mirror admitted aggregate helpers.
- Raw PySpark `WindowSpec` remains unsupported with diagnostics.

### M8E: Collection Helper PySpark Parity

Exit Criteria:

- Collection size, array membership, and map-key membership helpers are implemented.
- Array construction, repeat, union, and except helpers validate element types.
- Element lookup, safe element lookup, and map concatenation helpers document missing-key nullability, out-of-range
  array-index behavior, and duplicate-key behavior.
- Row-expanding generator helpers remain deferred unless separately admitted.

### M8F: Streaming Transformation Hardening

Exit Criteria:

- Watermarks, stateful dedupe, streaming aggregates, and admitted stream-stream shapes are compiler-visible and
  capability checked.
- Existing v1/v2 caller-owned streaming behavior remains valid, with documented source, sink, trigger, checkpoint,
  output-mode, and lifecycle ownership boundaries.

## M7E: Spark Connect Batch Support

### Exit Criteria

- `target_backend = "pyspark"` plus `target_variant = "spark-connect"` is documented as the supported Connect batch shape.
- Completed v1/v2 batch features have online and generated parity evidence for the Spark Connect variant.
- Classic-only internals fail through backend capability diagnostics before execution or generation.
- CI or a documented manual verification script proves Spark Connect execution against supported PySpark lines.
- Public docs explain supported batch behavior and the remaining streaming, storage-write, and hook-body exclusions.

## M9: v4 PySpark Transformation API Coverage

### Exit Criteria

- A checked catalog classifies every relevant PySpark 3.5.x/4.0.x transformation API as supported, scheduled, deferred,
  or unsupported, with a Structure alternative and reason.
- Supported APIs remain typed, symbolic, capability checked, explainable, readable when generated, and covered by
  online/generated parity evidence.
- Column, SQL-function, nested-value, relational, join, aggregate, window, and collection gaps are delivered in
  dependency order.
- Caller-owned streaming migration admits only session-window aggregation, bounded stream-stream outer and semi joins,
  and stream-static left-semi joins in Sprint 18, each with explicit state and output-mode diagnostics plus live
  evidence.
- Row generators are admitted only after an explicit schema-and-cardinality design proves their output shape safe.
- Loading, storage, actions, streaming orchestration, and alternative backends remain outside the milestone.
- The final v4 hardening sprint passes the release evidence, regression, parity, compatibility, generated-artifact,
  documentation, diagnostics, and performance-baseline checks without admitting new feature scope.

## M10: v5 Plugin Plugin Architecture

Status: complete. Sprints 19-22 delivered the public Plugin API, migrated the
bundled PySpark plugin, prove external-wheel isolation, validate default-denied private target-local engine
replacement, and close
the breaking v5 migration.

### Exit Criteria

- Core owns every public schema, compilation, execution, generation, serialization, capability, diagnostic, artifact,
  and CLI workflow; plugin code is invoked only through documented Plugin API service facets.
- Discovery reads installed distribution metadata without importing plugin modules and reports duplicate short ids
  deterministically.
- Core and the selected plugin negotiate the highest mutually supported Plugin API version; artifacts
  retain the negotiated version and reject incompatible consumers.
- A transform and composed pipeline resolve exactly one target, while a project can compile transforms assigned to
  different installed targets.
- Public PySpark plugin DSL names and field definitions are absent from the `structure` root and PySpark code imports
  them from
  `structure.plugin.pyspark`.
- PySpark schema handling, semantic checks, lowering, online execution, generated execution, rendering, capabilities,
  and diagnostics use the public Plugin API boundary without behavioral regression.
- External vendors can implement the published contract from their own packages and verify it with the conformance
  suite.
- The separately packaged finite-iterable plugin proves real entry-point discovery, API negotiation, target isolation,
  finite-generator execution, joins, grouped aggregation, collection, and opaque-plan serialization.
- The full build and supported PySpark integration matrix pass, and v5 migration, extension, troubleshooting, and
  release documentation are complete.

## +M11: v6 Typed PySpark API Closure, Example-Hook Retirement, and Bounded Recurrence

Status: complete. Sprints 23--26 delivered the v6 API ledger, PySpark plugin decomposition, Security hook
retirement, Search hook retirement, and bounded ordered timeline recurrence. Sprint 27 closed release evidence and
the challenge disposition register. Local release evidence records 1,313 passing tests and 40 intentional skips;
live PySpark lanes remain explicitly unclaimed in this workspace.

### Exit Criteria

- The PySpark API ledger classifies every remaining v4 catalog gap and every shipped-example raw hook as implemented,
  scheduled, deferred with a reason, or intentionally raw.
- The public PySpark façade is stable while the former fat modules are divided into focused operation, expression,
  symbolic-result, traceability, online-execution, step-rendering, and module-rendering delegates.
- Security's reconciliation hooks are replaced by ordinary typed step methods using lambda-bound struct fields; their
  generated code, execution plan, and traceability no longer expose opaque hook boundaries.
- Partitioned analytic maximum, deterministic ordered collection, exactly-one validation, and global aggregation have
  documented type/cardinality/empty-input semantics and online/generated/live evidence; aggregate-only methods retain
  global aggregation without requiring a preceding `group_by(...)` call.
- At least one shipped ordinary-PySpark `@special(type="udf")` example demonstrates its explicit return/nullability
  contract and warning behavior. It is not a fallback and remains excluded from Spark Connect.
- Typed generator, branch/set composition, self-alias, ordering, bound, relation-assertion (including parent
  reference), bounded-hierarchy/fallback, and declared-key priority-selection operations retire the corresponding Search hooks only where a same-fixture output
  comparison proves equivalence. Hooks that remain have an explicit P2 rationale.
- A normal transform consumes a caller-provided finite timeline and produces one typed output row per timeline row
  through `scan(...)`; the scan has declared state, partition/order keys, duplicate-key failure, and a positive bound.
- Generated and online PySpark use public DataFrame/Column APIs only: no implicit UDF, RDD, Pandas, action, driver
  loop, or raw-hook fallback claims typed feature support. Explicit user-authored scalar UDFs remain separately
  capability-checked and warning-governed.
- Challenges C27--C34 are resolved, deferred with a named follow-up, or escalated to an owner decision; C28/C30
  documentation and test obligations are fulfilled and C31 is clearly marked as awaiting project-owner authority if
  it remains undecided.
- `make build`, generated artifacts, capability/AST boundary checks, and required live PySpark 3.5/4.0 evidence pass.

## +M12: v7 Broad PySpark Transformation Coverage and Streaming Adoption

Status: complete. Sprints 28--35 completed the checked catalog, historical deferral reconciliation, focused
generator delegates, struct-generator expansion, Binary encoding, schema-carrying parsing, deterministic grouped
`mode(...)`, Stage One stream-static restart evidence, stream-static left-outer lookup hardening, single-stateful
streaming composition, raw-hook-bearing composition, and Search's first composed hook-bearing consumer. Streaming
coverage-percentage parity is the separate v8 milestone.

### Exit Criteria

- A checked PySpark 3.5.x/4.0.x catalog gives every reviewed transformation candidate a support state, contract,
  dependency, and evidence location.
- The admitted typed API families have schema/cardinality rules, capabilities, diagnostics, explain/traceability,
  readable generated output, online/generated parity, and live classic-PySpark 3.5/4.0 evidence.
- The oversized operation, expression, scope, result, evaluation, execution, rendering, and traceability modules have
  focused delegates or a documented cohesive-boundary decision, with behavior characterization at every extraction.
- The three kickoff-deferred transformation families—Binary encoding, Schema-carrying JSON/CSV conversion, and
  deterministic mode—have typed contracts, exact status rows, diagnostics, parity, and live classic-PySpark evidence.
- Caller-owned streaming progresses through independently verified stream-static enrichment, left-outer lookup, and a
  single-stateful-plus-stateless-composition stage with explicit watermark/state/output-mode rules, static diagnostics,
  file-stream restart evidence, and no Structure-owned lifecycle calls.
- Historical v4--v6 deferred work is either delivered by a named v7 slice or retained in an explicit backlog with its
  rationale; no stale plan is treated as active scope by implication.

## +M13: v8 PySpark Structured Streaming Coverage Parity

Status: complete. Sprints 36--39 published the checked coverage ledger, admitted typed struct generators and
exact-schema stream-stream unions, closed ordering and priority selection as explicit streaming-ineligible rows, and
recorded targeted live PySpark 3.5/4.0 restart evidence. Effective checked parity is 32 / 32. The governing plan is
`close/archive/planning/P07292601.V8-pyspark-structured-streaming-coverage-parity.plan.md`.

### Exit Criteria

- A checked Structured Streaming coverage ledger classifies every batch-supported PySpark catalog family as supported,
  partially supported, ineligible, or deferred for caller-owned streaming.
- The measured streaming coverage percentage is at least the current batch coverage percentage under the v8 measurement
  rule, with operation-level denominator splits where family-level rows mix Spark-supported and Spark-ineligible
  operations.
- Every admitted streaming operation has compiler-visible state/cardinality/output-mode rules, capability checks,
  diagnostics, explain/traceability, generated-source lifecycle scans, online/generated parity, and live PySpark
  3.5/4.0 restart evidence.
- Every Spark-ineligible or Structure-excluded streaming shape fails before query start with a corrective diagnostic and
  documentation link.
- Structure continues to return transformed DataFrames only; sources, sinks, checkpoints, triggers, output modes, query
  names, starts, stops, deployment, and recovery remain caller-owned.
- The final v8 hardening sprint refreshes documentation and generated artifacts, records release evidence, and passes
  `make build` without admitting new feature scope.

## +M14: v9 PySpark Streaming API Coverage and Adoption

Status: complete for coverage and adoption, with the V9 design-gate follow-up and release schedule active. Sprints
40--44 delivered the checked PySpark streaming API ledger, caller-owned adoption recipe, stateful/order-sensitive gap
reclassification, lifecycle diagnostics, documentation, live PySpark 3.5/4.0 recipe evidence, and initial build
evidence. Sprints 45--48 close the remaining design gates and harden the release.
The governing completed plan is
`close/archive/planning/P07292602.V9-pyspark-streaming-api-coverage.plan.md`, and the governing specification is
`docs/dev/specifications/V9PySparkStreamingApiCoverage.md`.

### Exit Criteria

- A checked PySpark Structured Streaming API ledger classifies every selected API family as Structure-supported,
  caller-owned-guided, design-gated, streaming-ineligible, or out of scope.
- The ledger separates typed DataFrame transformation support from caller-owned DataStreamReader, DataStreamWriter,
  checkpoint, trigger, output-mode, query lifecycle, side-effect, listener, arbitrary state, and Spark Connect
  streaming APIs.
- Runnable adoption examples show caller-created streaming sources and sinks around online and generated Structure
  transforms, including restart from caller-owned checkpoints.
- Streaming-related deferred items from v7 and v8 are implemented, explicitly rejected, design-gated, or retained with
  a current rationale.
- Design-gated streaming rows and non-streaming APICatalog planned/deferred rows have dedicated design,
  implementation specification, and active follow-up planning:
  `docs/dev/design/V9StreamingDesignGates.md`,
  `docs/dev/specifications/V9StreamingDesignGatedFeatures.md`,
  `docs/dev/design/V9ApiCatalogDesignGates.md`,
  `docs/dev/specifications/V9ApiCatalogDesignGatedFeatures.md`,
`docs/dev/planning/P07302601.V9-api-catalog-design-gates.plan.md`, and the Variant child plan
`docs/dev/planning/P07302602.V9-variant-type-and-helpers.plan.md`.
The released PySpark 4.0/4.2 Variant implementation slice is complete; PySpark 4.2 live probing remains an explicit
infrastructure follow-up, and 4.3+ mutation helpers remain design-gated until released profiles exist.
The dated execution schedule is
`docs/dev/planning/P07302603.V9-closeout-and-release.plan.md`.
- Diagnostics and explain output tell users whether a streaming issue should be fixed in Structure source,
  caller-owned lifecycle code, or a batch materialization boundary.
- Every admitted Structure-owned streaming claim has PySpark 3.5/4.0 live evidence, generated-source lifecycle scans,
  online/generated parity, documentation, and troubleshooting coverage.
- The final v9 hardening sprint records release evidence and passes `make build` without adding new API scope.
