# Sprint 28: V7 Scope, Coverage Catalog, and Streaming Design

## Sprint Goal

Turn the v7 direction into an implementation-ready PySpark coverage and caller-owned streaming program without admitting semantics before their contracts and live evidence are known.

## Product Outcome

Maintainers have one checked v7 backlog, a dependency-ordered feature sequence, explicit non-goals, and a tested design gate for the next streaming-adoption stage. Contributors can start a v7 feature slice without re-reading v4--v6 plans.

## Scope

### In Scope

- Publish the PySpark 3.5.x/4.0.x transformation coverage catalog and reconcile v4--v6 deferred items.
- Prioritize broad typed coverage across relational, projection, scalar/conditional, nested/collection, join, aggregate, and window families.
- Design the next caller-owned streaming stage, including state bounds, watermarks, output-mode diagnostics, file-stream restart evidence, and classic-PySpark target matrix.
- Characterize and plan the unfinished v6 delegate extraction across operation, expression, scope, result, evaluation, execution, rendering, and traceability components.
- Create the first design-ready execution slices and update roadmap, milestone, and backlog records.
- Extend v7 with typed admission plans for Binary encoding, Schema-carrying JSON/CSV conversion, and deterministic mode.
- Define the staged streaming-adoption sequence beyond the first feasibility gate.
- Deliver raw-hook-bearing `.to(...)` pipelines and `stage(...)` graphs with owner-qualified online/generated dispatch,
  caller-selected delegated or `embed_hooks` generated packaging, validation, traceability, and streaming
  compatibility parity.

### Out of Scope

- General raw PySpark wrapping, actions, reads/writes, catalog management, raw SQL, RDD/Pandas, arbitrary callbacks, or UDTFs.
- Structure-owned sources, sinks, triggers, checkpoints, output-mode calls, query lifecycle, or `foreachBatch`.
- Production incremental compilation, new external-plugin product scope, data-quality constraints, and Search-only evaluation work unless a later owner decision expands v7.
- Feature implementation beyond small, disposable feasibility fixtures.

## Governing Plan

`docs/dev/planning/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`

## Acceptance Criteria

- Every unresolved item from the reviewed v4, v5, and v6 plans has a v7 slice or an explicit retained-backlog disposition.
- The catalog records support status and contract facts for each selected PySpark API candidate.
- The next streaming stage has a written design and a live feasibility result for PySpark 3.5.x and 4.0.x.
- The first implementation sprint can begin with a self-contained specification and no unresolved public-semantic question.
- Raw-hook pipelines and stage graphs preserve source-stage hook ownership, honor the caller's `embed_hooks` preference
  for generated output, and pass online/generated parity before Search adopts the composed labeling pipeline.

## Progress

- [x] Close v6, archive its completed execution plan and Sprint 27 record, and consolidate unfinished historical work.
- [x] (2026-07-27) Published the v7 coverage specification and selected generator expansion as the first batch slice.
  The existing checked coverage JSON remains the canonical machine-readable catalog.
- [x] (2026-07-27) Published the caller-owned stream-static enrichment feasibility gate.
- [x] (2026-07-27) Published focused-delegate extraction boundaries for the generator slice. Characterization and the
  first implementation ExecPlan remain next.
- [x] (2026-07-27) Expanded v7 to commit the three deferred transformation families and three streaming-adoption
  stages; the delivery roadmap now spans Sprints 29--35.
- [x] (2026-07-28) Started implementation with the first behavior-preserving typed struct-generator extraction:
  generated rendering and online execution now use focused delegates, with existing `posexplode_struct(...)`
  characterization still passing.
- [x] (2026-07-28) Extracted generator streaming diagnostics and explain text to focused delegates, preserving the
  existing batch-only generator diagnostic and explain output.
- [x] (2026-07-28) Extracted generator compiler lowering to `MapPySparkGenerator`, preserving the existing
  `posexplode_struct(...)` operation recipe contract.
- [x] (2026-07-28) Completed the generator-adjacent extraction baseline: traceability is already isolated in
  `MapGeneratorTraceability`, and DSL capture now delegates validation and symbolic operation registration to
  `CapturePySparkGenerator`.
- [x] (2026-07-28) Added the first v7 generator helper, `explode_struct(...)`, through DSL capture, capability
  admission, lowering, generated rendering, online execution, explain, traceability, streaming diagnostics, tests, and
  the checked coverage catalog.
- [x] (2026-07-28) Added `explode_outer_struct(...)` with explicit outer recipe state and nullable generated-field
  validation, preserving the same compiler-visible generator path and batch-only streaming diagnostic.
- [x] (2026-07-28) Added `posexplode_outer_struct(...)` with nullable long ordinal validation, public rendering and
  online parity through `posexplode_outer`, traceability, streaming diagnostics, and catalog evidence.
- [x] (2026-07-28) Added `inline_struct(...)` and `inline_outer_struct(...)`, completing the focused non-live v7
  typed struct-generator family with compiler-owned temporary columns for generated and online parity.
- [x] (2026-07-28) Added live classic-PySpark evidence for the typed struct-generator family on PySpark 3.5 and 4.0,
  and fixed the ordered `scan(...)` nested higher-order lambda/state-array regression found during that evidence run.
- [x] (2026-07-28) Added V7 Stage One caller-owned streaming restart evidence for stream-static inner, left, and
  left-semi enrichment with test-owned file sources, Parquet sinks, and caller checkpoints on PySpark 3.5 and 4.0.
- [x] (2026-07-28) Added the v7 Binary encoding slice with public `binary()`/`BinaryType`, typed
  `base64(...)`, `unbase64(...)`, `encode(...)`, and `decode(...)`, schema import/render/materialization, focused
  contract tests, and live PySpark 3.5/4.0 parity evidence.
- [x] (2026-07-28) Added the v7 schema-carrying parsing slice with public `JsonOptions`/`CsvOptions`, typed
  `from_json(...)`, `to_json(...)`, `from_csv(...)`, and `to_csv(...)`, parser nullability diagnostics, generated
  source coverage, and live PySpark 3.5/4.0 parity evidence.
- [x] (2026-07-28) Added deterministic grouped `mode(...)` with PySpark-compatible public spelling,
  `deterministic=True` orderable tie semantics, grouped-only diagnostics, portable generated lowering, and live
  PySpark 3.5/4.0 parity evidence.
- [x] (2026-07-28) Delivered raw-hook-bearing transform composition: stage-owned hooks now survive `.to(...)` and
  `stage(...)`, delegated generated code constructs stage-local hook delegates, `embed_hooks` emits owner-qualified
  copied hooks, and focused coverage proves repeated same-class delegates, online dispatch, traceability, and streaming
  reporting.
- [x] (2026-07-28) Moved the Search query-labeling workflow onto the generic composition path with `LabelQueries`, and
  kept production scoring behind the stable `score.ScoreAll` facade while using a composed scoring pipeline internally.
- [x] (2026-07-28) Closed Sprint 28 release evidence: `make build` passed with mypy clean, main pytest
  `1361 passed, 47 skipped`, secondary pytest `34 passed, 6 skipped`, and wheel/sdist artifacts built.
