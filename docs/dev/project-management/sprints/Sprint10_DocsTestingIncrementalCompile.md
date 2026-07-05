# Sprint 10: Generated Docs, Test Tooling, and Incremental Compile

## Sprint Goal

Finish the adoption tooling deferred from Sprint 09: generated documentation artifacts, public pytest helpers, and
production incremental compile with cache diagnostics.

## Product Outcome

Developers can keep generated artifacts fresh in CI, publish schema and transform reference material automatically,
and get fast feedback in large projects without recompiling unaffected transforms.

## Scope

### In Scope

- Generated Markdown or JSON documentation artifacts for schemas and transforms.
- Pytest helpers for compiler checks, generated-code freshness, generated-code snapshots, expected diagnostics, and
  online/generated parity.
- Production incremental compile with `compile --changed-only`, cache invalidation, and cache diagnostics.
- Performance fixtures for incremental compile on synthetic 10-transform and 100-transform projects.

### Out of Scope

- Optimization directives and rich explain work covered by Sprint 09.
- Spark Connect batch support promotion covered by Sprint 09.
- Streaming source and sink generation.

## Relevant Specification Items

- As a developer, I can generate documentation artifacts for schemas and transforms.
- As a developer, I can use pytest helpers for compiler checks, freshness, snapshots, diagnostics, and parity.
- As a developer, I can use production incremental compilation.

## Engineering Tasks

1. Add generated documentation artifact emitter.
2. Add docs configuration for Markdown and JSON output destinations.
3. Add pytest helpers for compiler success, expected diagnostics, generated freshness, snapshots, and parity.
4. Implement `compile --changed-only`.
5. Add cache invalidation rules for source, config, schema, dependency, and generated-target changes.
6. Add cache diagnostics and performance fixtures.

## Acceptance Criteria

- Generated docs summarize schemas, transform inputs, outputs, subtransforms, dependencies, and target artifacts.
- Pytest helpers let downstream projects assert compiler success, expected diagnostics, generated freshness, snapshots,
  and online/generated parity.
- `compile --changed-only` recompiles changed transforms and affected dependents without hiding stale output.
- Cache diagnostics explain why each transform was reused or recompiled.

## Progress

- [ ] Implement generated docs.
- [ ] Implement pytest helpers.
- [ ] Implement incremental compile and cache diagnostics.

## Compile-Time Performance Metric

Track cold and warm incremental compile time.

Targets:

- A no-change `compile --changed-only` on a 100-transform synthetic project completes in under 2 seconds excluding
  interpreter startup.
- A one-transform change recompiles only the changed transform and affected dependents.

## Risks

- Incremental compile can be worse than full compile if cache invalidation rules are vague.
- Public pytest helpers can overfit to Structure internals instead of stable behavior.
- Generated documentation can become noisy if it mirrors compiler internals instead of user contracts.

## Notes

Keep generated docs and pytest helpers focused on public contracts. Internal traceability remains useful, but the
public adoption surface should read like library documentation, not a compiler dump.
