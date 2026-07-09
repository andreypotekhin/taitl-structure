# Sprint 10: Generated Docs and Test Tooling

## Sprint Goal

Finish the adoption tooling deferred from Sprint 09: generated documentation artifacts and public pytest helpers.
Production incremental compile with cache diagnostics moved to the end of v3.

## Product Outcome

Developers can keep generated artifacts fresh in CI and publish schema and transform reference material automatically.

## Scope

### In Scope

- Generated Markdown or JSON documentation artifacts for schemas and transforms.
- Pytest helpers for compiler checks, generated-code freshness, generated-code snapshots, expected diagnostics, and
  online/generated parity.

### Out of Scope

- Broader optimization directives and rich field-level explain work unless explicitly pulled in from the post-Sprint 09
  follow-up backlog.
- Spark Connect batch support promotion covered by Sprint 09.
- Streaming source and sink generation.
- Production incremental compile with `compile --changed-only`, cache invalidation, cache diagnostics, and performance
  fixtures. This moved to Sprint 17 at the end of v3.

## Relevant Specification Items

- As a developer, I can generate documentation artifacts for schemas and transforms.
- As a developer, I can use pytest helpers for compiler checks, freshness, snapshots, diagnostics, and parity.

## Engineering Tasks

1. Add generated documentation artifact emitter.
2. Add docs configuration for Markdown and JSON output destinations.
3. Add pytest helpers for compiler success, expected diagnostics, generated freshness, snapshots, and parity.

## Acceptance Criteria

- Generated docs summarize schemas, transform inputs, outputs, subtransforms, dependencies, and target artifacts.
- Pytest helpers let downstream projects assert compiler success, expected diagnostics, generated freshness, snapshots,
  and online/generated parity.
- Incremental compile and cache diagnostics are explicitly scheduled into Sprint 17.

## Progress

- [x] Implement generated docs.
- [x] Implement pytest helpers.
- [x] Move incremental compile and cache diagnostics to end-of-v3 Sprint 17.

Generated docs first slice is implemented through `structure compile`. The compiler now writes Markdown and JSON
schema/transform reference artifacts under `generated_docs_dir` inside `generated_dir`, with configurable
`generated_docs_formats`. The artifacts summarize schema fields, transform inputs, outputs, subtransforms,
dependencies, and target artifacts; freshness uses the existing generated-file compare/write path.

Pytest helper slice is implemented through `structure.lib.testing`. Downstream projects can assert compiler success,
generated-code freshness, generated snapshots, expected diagnostics, and online/generated parity without importing
fixture-specific repository helpers or PySpark at test collection time.

## Risks

- Public pytest helpers can overfit to Structure internals instead of stable behavior.
- Generated documentation can become noisy if it mirrors compiler internals instead of user contracts.

## Notes

Keep generated docs and pytest helpers focused on public contracts. Internal traceability remains useful, but the
public adoption surface should read like library documentation, not a compiler dump.
