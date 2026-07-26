# Sprint 23: V6 API Ledger and PySpark Plugin Decomposition

## Sprint Goal

Make the remaining PySpark transformation frontier reviewable and make its implementation boundaries small enough to
change safely.

## Product Outcome

Maintainers and users can see why every remaining API or example raw hook is supported, scheduled, deferred, or
intentional. Subsequent v6 features land in focused PySpark components without changing the public DSL or unrelated
renderer/executor behavior.

## Scope

### In Scope

- Publish the v6 API ledger and raw-hook inventory derived from the transformation coverage catalog and examples;
  synchronize every postponed/deferred disposition with `docs/dev/Gaps.md`.
- Characterize generated, recipe, online, traceability, and public-import behavior before each extraction.
- Extract focused delegates from the oversized PySpark operation, expression, scope, result, evaluation, execution,
  rendering, and traceability modules.
- Preserve the PySpark public façade and existing endpoint-only cross-app boundary.
- Create executable fixtures for Security and Search migration prerequisites.

### Out of Scope

- New public transformation helpers or changes to PySpark semantics.
- Replacing a raw hook.
- Revising the v5 Plugin API or accepting a private Core import from PySpark.

## Governing Plan

`docs/dev/planning/P07242604.V6-pyspark-api-and-example-hook-retirement.plan.md`

## Acceptance Criteria

- The ledger gives every deferred/unsupported catalog item and every example `@raw` method a status, rationale,
  owner sprint, contract link, and evidence location.
- `docs/dev/Gaps.md`, the coverage catalog, and the ledger agree on every v6 postponed/deferred status.
- Extracted components have one coherent responsibility and direct characterization coverage.
- Generated snapshots, public imports, online recipes, and traceability remain behaviorally identical.
- No new PySpark module imports a private `structure.core` implementation or another PySpark app's `commands`/`logic`
  package.
- `make build` passes.

## Risks and Controls

- Extraction drift: make one focused extraction per change, compare generated artifacts, and run parity tests before
  any semantic change.
- Decorative catalog work: the raw-hook inventory test makes ledger omissions fail rather than relying on prose.

## Progress

- [x] (2026-07-26) Started v6 after M10/v5 closeout. Sprint 23 is the active iteration; its governing ledger and
  executable plan are the release baseline.
- [x] (2026-07-26) Published the checked raw-hook inventory. It records all thirteen example hooks, their
  retirement/defer/intentional disposition, owner sprint, and required capability; an AST-based specification test
  rejects inventory drift.
- [x] (2026-07-26) Added executable characterization coverage for the checked hook inventory and nested Python-UDF
  boundary discovery; existing generated-source, public-import, online-recipe, and compiler-traceability suites remain
  the behavior guards for the extracted components.
- [x] Extract focused delegates with no behavior change.
  - [x] (2026-07-26) Extracted cache directive capture and StorageLevel validation into
    `dsl/operations/CacheOperations.py`; the public façade remains stable and focused cache/render/fixture coverage
    passed.
  - [x] (2026-07-26) Extracted recursive Python-UDF-boundary discovery into
    `compiler/logic/traceability/FindPythonUdfBoundaries.py`; compiler traceability remains byte-for-byte equivalent
    under direct and end-to-end tests.
  - [x] (2026-07-26) Extracted join provenance and dataflow mapping into
    `compiler/logic/traceability/MapJoinTraceability.py`; all join-cardinality, hint, temporal, and as-of records
    remain covered by compiler traceability tests.
  - [x] (2026-07-26) Extracted duplicate-removal provenance and dataflow mapping into
    `compiler/logic/traceability/MapDeduplicationTraceability.py`; current-frame and subset-based dependencies retain
    their existing traceability contract.
  - [x] (2026-07-26) Extracted selected-row window provenance and dataflow mapping into
    `compiler/logic/traceability/MapSelectedRowsTraceability.py`; partition, ordering, and tie-policy dependencies
    remain covered by compiler traceability tests.
  - [x] (2026-07-26) Extracted filter provenance and dataflow mapping into
    `compiler/logic/traceability/MapFilterTraceability.py`; the focused traceability suite and compiler static
    analysis preserve filter records without changing the report assembler.
  - [x] (2026-07-26) Extracted aggregate and post-aggregate-predicate traceability into
    `compiler/logic/traceability/MapAggregateTraceability.py`; aggregate source collection and having records retain
    their existing focused contract.
  - [x] (2026-07-26) Extracted ordinary and multi-result projection traceability into
    `compiler/logic/traceability/MapProjectionTraceability.py`; lane-aware dependencies and generated source paths
    remain covered by the compiler-traceability contract.
  - [x] (2026-07-26) Extracted raw-hook provenance, dataflow, and opaque-boundary mapping into
    `compiler/logic/traceability/MapHookTraceability.py`; before/after and result-lane hook contracts remain intact.
  - [x] (2026-07-26) Extracted step and final-output validation traceability into
    `compiler/logic/traceability/MapValidationTraceability.py`; validation records remain covered by their compiler
    and generated-code contracts.
  - [x] (2026-07-26) Extracted declared return-shape validation from result-body construction into
    `symbolic_execution/logic/results/ValidatePySparkResultReturn.py`; single and multi-output contracts retain
    their existing diagnostics and symbolic-execution coverage.
  - [x] (2026-07-26) Extracted predicate rendering from `RenderPySparkStep` into
    `render/logic/steps/RenderPySparkFilters.py`; ordered predicate batches retain their generated-source contract.
  - [x] (2026-07-26) Extracted aggregate-plan rendering from `RenderPySparkStep` into
    `render/logic/steps/RenderPySparkAggregatePlan.py`; global aggregation, group-by, rollup, cube, and grouping-set
    orchestration remain covered by generated-source contracts.
- [x] (2026-07-26) Added executable Security/Search migration prerequisites: every ledgered hook compiles into its
  declared opaque boundary, Security's raw reconciliation field/predicate contract is checked, and the Gaps-side
  v6 register is checked against the ledger's postponed and scheduled capability names.
- [x] Run release regression evidence.
  - [x] (2026-07-26) `make build` passed: 1,172 tests passed and 29 live-PySpark tests skipped; the release subset
    passed 33 tests with 6 intentional live-test skips. Source and wheel distributions built successfully.
  - [x] (2026-07-26) Re-ran `make build` after projection, hook, and validation extraction: 1,175 tests passed and
    29 live-PySpark tests skipped; the release subset passed 34 tests with 6 intentional live-test skips. Source and
    wheel distributions built successfully.
  - [x] (2026-07-26) Re-ran `make build` after result-return validation extraction: 1,175 tests passed and 29
    live-PySpark tests skipped; the release subset passed 34 tests with 6 intentional live-test skips. Source and
    wheel distributions built successfully.
  - [x] (2026-07-26) Re-ran `make build` after filter-rendering extraction: 1,175 tests passed and 29 live-PySpark
    tests skipped; the release subset passed 34 tests with 6 intentional live-test skips. Source and wheel
    distributions built successfully.
  - [x] (2026-07-26) Re-ran `make build` after aggregate-plan rendering extraction: 1,175 tests passed and 29
    live-PySpark tests skipped; the release subset passed 34 tests with 6 intentional live-test skips. Source and
    wheel distributions built successfully.
  - [x] (2026-07-26) Sprint-closeout `make build` passed: 1,178 tests passed and 29 live-PySpark tests skipped; the
    release subset passed 34 tests with 6 intentional live-test skips. Source and wheel distributions built
    successfully.

## Outcome

Completed 2026-07-26. The v6 ledger, checked Gaps register, raw-hook inventory, and migration fixtures now make the
Sprint 24 Security and Sprint 25 Search work reviewable without adding semantics prematurely. Further decomposition
is deliberately deferred until a feature slice requires it.
