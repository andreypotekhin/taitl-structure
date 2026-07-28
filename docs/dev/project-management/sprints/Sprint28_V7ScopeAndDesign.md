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

## Progress

- [x] Close v6, archive its completed execution plan and Sprint 27 record, and consolidate unfinished historical work.
- [x] (2026-07-27) Published the v7 coverage specification and selected generator expansion as the first batch slice.
  The existing checked coverage JSON remains the canonical machine-readable catalog.
- [x] (2026-07-27) Published the caller-owned stream-static enrichment feasibility gate; live target evidence remains
  the next task.
- [x] (2026-07-27) Published focused-delegate extraction boundaries for the generator slice. Characterization and the
  first implementation ExecPlan remain next.
- [x] (2026-07-27) Expanded v7 to commit the three deferred transformation families and three streaming-adoption
  stages; the delivery roadmap now spans Sprints 29--35.
