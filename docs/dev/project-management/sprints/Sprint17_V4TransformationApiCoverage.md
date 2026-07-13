# Sprint 17: V4 Transformation API Coverage Foundation

## Sprint Goal

Make Structure's PySpark transformation coverage predictable before adding another broad helper slice. The sprint
creates the checked catalog, baseline inventory, status rules, and fixture that govern the rest of v4.

## Product Outcome

Developers can inspect a relevant PySpark 3.5.x/4.0.x transformation API and see whether Structure supports it,
plans it, defers it with a reason, or deliberately excludes it. Maintainers cannot silently leave an in-scope API
unclassified.

## Scope

### In Scope

- A checked local inventory of relevant Column, SQL-function, and DataFrame transformation APIs.
- A public transformation coverage catalog with support status, Structure spelling or alternative, target profile,
  semantics, and evidence links.
- Catalog integrity tests and a v4 fixture skeleton.
- Reclassification of the current planned gaps against the catalog.
- User stories, API reference, gaps, and diagnostics vocabulary required to make the catalog useful.

### Out of Scope

- Loading, storage, actions, streaming lifecycle ownership, and table/catalog management.
- Alternative backend work and non-batch Spark Connect hardening.
- Bulk helper implementation before the catalog and its admission gate exist.

## ExecPlan

`docs/dev/planning/P07132601.V4-transformation-api-coverage.plan.md`

## Engineering Tasks

1. Define the local PySpark 3.5.x/4.0.x transformation inventory and catalog format.
2. Add catalog completeness and supported-entry evidence tests.
3. Publish the public catalog and v4 fixture skeleton.
4. Classify current gaps and schedule the first helper family from the result.

## Acceptance Criteria

- Every in-scope baseline transformation API has exactly one status.
- Every status offers either a Structure spelling, an explicit future slice, or a usable alternative.
- A claimed supported API links to target and parity evidence.
- Compiler-only catalog checks remain Spark-free.
- `make build` passes.

## Progress

- [ ] Implement the coverage inventory, catalog, tests, and fixture skeleton.
