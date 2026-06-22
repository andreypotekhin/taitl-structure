# Milestones

## M0: Groundwork Ready

Status: in progress. Configuration resolution, CLI entrypoint, seed config generation, Spark-free `check`, Spark-free
`compile`, and generated-output diff checks are implemented and tested. Remaining closeout items include spike-note
evidence and CI workflow documentation before this milestone should be marked with `+`.

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

Status: in progress. The shared PySpark recipe layer, generated transform rendering, public `StructureSession`, deferred
input binding, generated runner delegation, runtime input diagnostics, live online PySpark recipe interpretation, and
online/generated row parity integration coverage are implemented. Remaining closeout is validating the PySpark
integration matrix in an environment with PySpark installed, because the local workspace skips those tests.

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

Status: in progress. Spark schema source rendering, generated schema modules, generated runtime schema helpers,
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

## M3: Expression DSL Usability

Status: in progress. v1 fixture expressions, filters, expression helpers, generated expression rendering, and the
shared diagnostic registry are implemented and tested. Unsupported Python operation diagnostics and live
online/generated expression parity integration coverage are now in place. Remaining closeout is expanding negative
diagnostics beyond the representative v1 cases.

### Exit Criteria

- Common expression functions compile.
- `where(...)` filters compile.
- `@expr_fn` helpers compile and inline.
- Unsupported Python operations fail with structured diagnostics.
- Error messages suggest DSL alternatives, `@expr_fn`, hooks, and config workarounds when applicable.

## M4: Hooks and Generated Classes

### Exit Criteria

- Online execution is the primary runtime path.
- Generated classes remain optional artifacts.
- Hook-free transforms do not import source transform classes.
- Hooked transforms direct-import source class and call hooks.
- Hook signature is validated.
- `@after(method)` and `@before(method)` work.

## M5: Joins, Compiler Traceability, Build Integration

Status: in progress. `join_one(...)`, source-order join lowering, generated join rendering, `compile --fail-on-diff`,
compact `structure explain`, streaming compatibility reporting, compiler provenance, static dataflow traceability artifacts,
compact explain traceability summaries, and online/generated join parity integration coverage are implemented and tested.
Remaining exit criteria are broader CI build-integration coverage and validating the live PySpark matrix outside this
PySpark-free workspace.

### Exit Criteria

- `join_one(...)` compiles to PySpark joins.
- N-step serial joins work across arbitrary named inputs.
- Compiler provenance maps source nodes to IR nodes to generated PySpark nodes.
- Static dataflow traceability shows transform, table, and column dependencies inferred from IR.
- `structure compile --fail-on-diff` works.
- `structure explain` summarizes inputs, steps, filters, joins, hooks, and validation.
- Streaming compatibility reports whether transforms are compatible, batch-only, or unknown.

## M6: v1 Stabilization

Status: in progress. The first registry-backed diagnostic contract is implemented with public anchors, renderer,
registry validation tests, and representative routing for configuration, backend capability, generated-output drift,
runtime, and CLI internal errors. Remaining stabilization work includes broader compiler negative diagnostics,
multi-version PySpark strategy, generated-code version headers, and setup/doctor checks.

### Exit Criteria

- Compatibility docs, generated-code version headers, compiler traceability schema versioning, and config schema
  compatibility are checked against release artifacts.
- Multi-version PySpark test strategy covers the documented v1 target range.
- Diagnostic codes link to relevant documentation.
- Setup/configuration doctor checks the common adoption failures.

## M7: v2 Analytical Pipeline Features

### Exit Criteria

- Windowing and deduplication helpers cover latest-row, ranking, lag/lead, and duplicate-removal use cases.
- Typed aggregation support covers common group-by rollups before advanced grouping sets.
- Higher-order array/map helpers remain Spark-plan-visible.
- Manual optimization directives are explicit in source and obvious in generated code.
- Existence joins cover semi and anti filter semantics without exposing right-side fields.
- `join_many(...)` has clear row-multiplication semantics and online/generated parity tests.
- Deterministic lookup dedupe policies never rely on arbitrary right-row selection.
- Temporal validity-window joins support SCD-style lookups with explicit overlap policy.
- Backward as-of joins support time-relative enrichment with optional tolerance.
- Richer explain output, generated documentation artifacts, and pytest helpers improve adoption without adding runtime
  responsibility.
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
