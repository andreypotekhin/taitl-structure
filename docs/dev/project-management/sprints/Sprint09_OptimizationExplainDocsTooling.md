# Sprint 09: Spark Connect, Optimization, and Explain

## Sprint Goal

Build directly on Sprint 08 analytical operations by proving the completed batch feature set against the experimental
Spark Connect variant, adding explicit optimization directives, and making explain output rich enough to review
aggregation, selected-row, dedupe, and higher-order pipelines.

## Product Outcome

Developers can try completed batch transforms with Spark Connect, request important Spark physical-plan hints
explicitly, and inspect analytical dataflow without reading generated code line by line.

## Scope

### In Scope

- Experimental Spark Connect parity checks for completed batch features.
- Cache and persistence directives at subtransform boundaries.
- Repartition and coalesce directives.
- Checkpoint hints where the configured backend supports them.
- Join strategy directives for broadcast, shuffle hash, sort merge, and lookup projection where supported.
- Rich `structure explain` mode for field-level lineage.

### Out of Scope

- Automatic cost-based optimization.
- Automatic join reordering.
- Storage write orchestration.
- Full supported Spark Connect promotion.
- Streaming source and sink generation.
- Generated documentation artifacts.
- Public pytest helper package.
- Production incremental compile and cache diagnostics.

## Relevant Specification Items

- As a developer, I can add caching and persistence hints at step boundaries.
- As a developer, I can add repartition and coalesce hints.
- As a developer, I can add checkpoint hints where supported.
- As a developer, I can specify join strategies and hints.
- As a developer, I can generate richer static dataflow explain output.
- As a developer, I can explain generated-code sections.
- As a developer, I can run experimental Spark Connect parity checks for completed batch features.

## Engineering Tasks

1. Add Spark Connect experimental parity checks at the beginning of the sprint for completed batch features.
2. Document and enforce Spark Connect exclusions for classic-only internals.
3. Implement optimization directive source capture and IR.
4. Add backend capability checks and diagnostics for each directive.
5. Render directives in online and generated PySpark through shared recipes.
6. Add tests proving directives do not change row or schema semantics.
7. Add rich explain output for field-level lineage through projections, filters, joins, aggregations, selected-row
   helpers, exact/subset dedupe, higher-order expressions, hooks, and optimization boundaries.
8. Add generated-code section labels where they help explain reports point to emitted code.

## Acceptance Criteria

- Optimization directives are visible in source, IR, generated code, traceability, and explain output.
- Unsupported directives fail with backend capability diagnostics before runtime.
- Rich explain output can follow field lineage through projections, filters, joins, aggregations, windows, hooks, and
  optimization boundaries.
- Experimental Spark Connect parity covers completed batch features without changing public DSL or generated
  class APIs.

## Progress

- [ ] Add experimental Spark Connect parity for completed batch features.
- [ ] Implement explicit optimization directives.
- [ ] Implement rich explain output for completed batch operations.

## Explain Performance Metric

Track explain rendering time for analytical fixture transforms.

Targets:

- A 10-transform analytical fixture explain report renders in under 2 seconds excluding interpreter startup.
- Explain output remains compact by default and expands only when requested.

## Risks

- Optimization directives can imply guarantees Spark does not make.
- Rich explain output can become noisy if compact summaries are not the default.
- Spark Connect parity can become noisy if classic-only exclusions are not explicit.

## Notes

Keep optimization directives honest: they are explicit user intent and backend requests, not promises that Spark will
always choose a particular physical plan. Keep Spark Connect experimental: this sprint proves parity for completed
batch features, not general support.
