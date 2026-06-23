# Sprint 09: Optimization, Explain, Docs, and Test Tooling

## Sprint Goal

Make v2 analytical pipelines practical to adopt at project scale by adding explicit optimization directives, richer
explain output, generated documentation, pytest helpers, and incremental compile.

## Product Outcome

Developers can review complex generated analytical code, keep generated artifacts fresh in tests and CI, document the
schema and transform contract automatically, and get fast feedback in large projects.

## Scope

### In Scope

- Cache and persistence directives at subtransform boundaries.
- Repartition and coalesce directives.
- Checkpoint hints where the configured backend supports them.
- Join strategy directives for broadcast, shuffle hash, sort merge, and lookup projection where supported.
- Rich `structure explain` mode for field-level lineage.
- Generated Markdown or JSON documentation artifacts for schemas and transforms.
- Pytest helpers for compiler checks, generated-code freshness, generated-code snapshots, expected diagnostics, and
  online/generated parity.
- Production incremental compile with `compile --changed-only`, cache invalidation, and cache diagnostics.
- Performance fixtures for incremental compile on synthetic 10-transform and 100-transform projects.

### Out of Scope

- Automatic cost-based optimization.
- Automatic join reordering.
- Storage write orchestration.
- Spark Connect support.
- Streaming source and sink generation.

## Relevant Specification Items

- As a developer, I can add caching and persistence hints at step boundaries.
- As a developer, I can add repartition and coalesce hints.
- As a developer, I can add checkpoint hints where supported.
- As a developer, I can specify join strategies and hints.
- As a developer, I can generate richer static dataflow explain output.
- As a developer, I can explain generated-code sections.
- As a developer, I can generate documentation artifacts for schemas and transforms.
- As a developer, I can use pytest helpers for compiler checks, freshness, snapshots, diagnostics, and parity.
- As a developer, I can use production incremental compilation.

## Engineering Tasks

1. Implement optimization directive source capture and IR.
2. Add backend capability checks and diagnostics for each directive.
3. Render directives in online and generated PySpark through shared recipes.
4. Add tests proving directives do not change row or schema semantics.
5. Add rich explain output for field-level lineage through v1 and v2 operations.
6. Add generated documentation artifact emitter.
7. Add pytest helpers for compiler checks, generated freshness, snapshots, diagnostics, and parity.
8. Implement `compile --changed-only`.
9. Add cache invalidation rules for source, config, schema, dependency, and generated-target changes.
10. Add cache diagnostics and performance fixtures.

## Acceptance Criteria

- Optimization directives are visible in source, IR, generated code, traceability, and explain output.
- Unsupported directives fail with backend capability diagnostics before runtime.
- Rich explain output can follow field lineage through projections, filters, joins, aggregations, windows, hooks, and
  optimization boundaries.
- Generated docs summarize schemas, transform inputs, outputs, subtransforms, dependencies, and target artifacts.
- Pytest helpers let downstream projects assert compiler success, expected diagnostics, generated freshness, snapshots,
  and online/generated parity.
- `compile --changed-only` recompiles changed transforms and affected dependents without hiding stale output.

## Progress

- [ ] Implement explicit optimization directives.
- [ ] Implement rich explain output and generated docs.
- [ ] Implement pytest helpers.
- [ ] Implement incremental compile and cache diagnostics.

## Compile-Time Performance Metric

Track cold and warm incremental compile time.

Targets:

- A no-change `compile --changed-only` on a 100-transform synthetic project completes in under 2 seconds excluding
  interpreter startup.
- A one-transform change recompiles only the changed transform and affected dependents.

## Risks

- Optimization directives can imply guarantees Spark does not make.
- Rich explain output can become noisy if compact summaries are not the default.
- Incremental compile can be worse than full compile if cache invalidation rules are vague.

## Notes

Keep optimization directives honest: they are explicit user intent and backend requests, not promises that Spark will
always choose a particular physical plan.
