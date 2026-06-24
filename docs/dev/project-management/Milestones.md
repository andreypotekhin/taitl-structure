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
runtime schema materialization, validation recipe placement, and online-materialized `transform.schemas.output`
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
- `@expr_fn` helpers compile and inline.
- Unsupported Python operations fail with structured diagnostics.
- Error messages suggest DSL alternatives, `@expr_fn`, hooks, and config workarounds when applicable.

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
- `@after(method, lane=lane)` and `@before(method, lane=lane)` work.

## M5: Joins, Compiler Traceability, Build Integration

Status: v1 external validation. `join_one(...)`, source-order join lowering, generated join rendering, uniqueness
warnings, stricter join-condition/key diagnostics, `compile --fail-on-diff`, compact `structure explain`, streaming
compatibility reporting, compiler provenance, static dataflow traceability artifacts, compact explain traceability
summaries, and online/generated join parity integration coverage are implemented and tested. Remaining exit criteria
are broader CI build-integration coverage and validating the live PySpark matrix outside this PySpark-free workspace.

### Exit Criteria

- `join_one(...)` compiles to PySpark joins.
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

Status: planned. v2 starts after v1 stabilization evidence is release-ready. The milestone is split into M7A-M7D so
independent contributors can work on analytical transforms, analytical joins, and adoption tooling without stepping on
one another.

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

Exit Criteria:

- Existence joins cover semi and anti filter semantics without exposing right-side fields.
- `join_many(...)` has clear row-multiplication semantics and online/generated parity tests.
- Deterministic lookup dedupe policies never rely on arbitrary right-row selection.
- Temporal validity-window joins support SCD-style lookups with explicit overlap policy.
- Backward as-of joins support time-relative enrichment with optional tolerance.
- Traceability and `structure explain` show row-filtering, row-multiplying, select-one, temporal, and as-of cardinality.

### M7C: Aggregations, Windows, and Higher-Order Functions

Exit Criteria:

- Typed `group_by(...)` and aggregation support covers count, sum, min, max, average, distinct count where practical,
  and schema-checked aggregate output construction.
- Windowing covers latest-row, ranking, lag/lead, rolling metrics, and duplicate-removal helpers.
- Deduplication helpers expose deterministic tie policies and never lower to arbitrary `dropDuplicates(...)` when a
  selected row matters.
- Spark higher-order helpers for arrays and maps remain Spark-plan-visible and reject unsupported Python callbacks.
- Online/generated parity tests cover every admitted aggregation, window, dedupe, and higher-order helper form.

### M7D: Optimization, Explain, Docs, and Test Tooling

Exit Criteria:

- Manual optimization directives are explicit in source and obvious in generated code.
- Cache, persist, repartition, coalesce, checkpoint, and join strategy directives are represented in IR and backend
  capability checks.
- Richer static dataflow explain output can show field lineage through projections, filters, joins, aggregations,
  windows, hooks, and optimization boundaries.
- Generated documentation artifacts describe schemas, transforms, inputs, outputs, traceability, and generated targets
  in Markdown or JSON.
- Pytest helpers cover `structure check`, generated-code freshness, generated-code snapshots, diagnostics, and
  online/generated parity fixtures.
- Production incremental compile has cache invalidation tests and diagnostics.

## M8: v3 Streaming Orchestration

### Exit Criteria

- Streaming sources and sinks are declared explicitly.
- Generated `readStream` and `writeStream` code is reviewable.
- Triggers, checkpoints, output modes, watermarks, and state policies are modeled and tested.
- Existing v1/v2 streaming compatibility behavior remains valid for caller-owned streaming orchestration.

## M9: v4 Spark Connect

### Exit Criteria

- Spark Connect support has a tested online/generated contract.
- Public docs explain the difference between ordinary PySpark and Spark Connect targets.
- Backend capability reporting prevents accidental use of unsupported APIs.
