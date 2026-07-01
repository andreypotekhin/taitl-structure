# Sprint 06: v2 Scope and Analytical IR Foundations

## Sprint Goal

Define the v2 release boundary and prepare the shared compiler foundations needed by every v2 analytical feature.

## Product Outcome

Developers and contributors can see exactly what v2 owns, which features are deferred, and how new analytical
operations will flow through source capture, IR, backend capability checks, online execution, generated PySpark,
diagnostics, traceability, and tests.

## Scope

### In Scope

- Published v2 scope, non-goals, user stories, backlog epics, milestones, and sprint sequence.
- v2 operation taxonomy for aggregation, window, higher-order function, optimization directive, and analytical join
  operations.
- IR fields for operation kind, source location, backend capability, cardinality, streaming compatibility, traceability,
  and output schema.
- Shared PySpark recipe seams for online/generated parity.
- v2 fixture package for small orders-style examples.
- Diagnostic code placeholders and public documentation anchors for unsupported v2 operation shapes.
- Compile-time performance baselines for v2 fixtures.

### Out of Scope

- Full lowering for aggregation, windowing, higher-order functions, or analytical joins.
- Streaming orchestration.
- Spark Connect support.
- Automatic cost-based optimization or join reordering.

## Relevant Specification Items

- As a developer, I can see a published v2 scope and non-goals.
- As a developer, I can receive backend capability diagnostics for every v2 operation.
- As a developer, I can inspect v2 operation cardinality in explain output.
- As a developer, I can rely on online and generated execution using the same v2 PySpark recipe layer.
- As a developer, I can keep caller-owned streaming orchestration in v2.

## Engineering Tasks

1. Update roadmap, milestones, backlog, user stories, and traceability documents for v2.
2. Add v2 fixture package skeleton for aggregation, windowing, arrays/maps, optimization hints, and analytical joins.
3. Define v2 operation categories in the IR.
4. Add backend capability names for v2 operation categories.
5. Add cardinality classification values for row-preserving, row-filtering, row-multiplying, aggregate, and select-one
   operations.
6. Add v2 streaming compatibility classification stubs.
7. Add diagnostic anchors for unsupported v2 operation categories.
8. Add parity harness placeholders that later sprints can fill with concrete cases.

## Acceptance Criteria

- v2 scope and non-goals are visible in project-management and user-story documents.
- Each v2 feature family has an owning milestone and sprint.
- A future implementation can add a v2 operation without inventing a new IR metadata convention.
- Backend capability errors can name the unsupported v2 operation family.
- `structure explain` has a planned place to show v2 operation cardinality.
- v2 fixtures are small enough to review and broad enough to cover every v2 sprint.

## Progress

- [x] (2026-06-23) v2 scope, user stories, backlog epics, milestone split, and sprint charters are drafted.
- [x] (2026-07-01) Added v2 fixture package skeleton and importability coverage for source fixtures.
- [x] (2026-07-01) Added IR operation metadata for capability, cardinality, and streaming support placeholders.
- [x] (2026-07-01) Added unsupported v2 backend capability checks and explain cardinality anchors.
- [ ] Add compile-time performance baseline for the v2 fixture package.

## Compile-Time Performance Metric

Track `structure check` and `structure compile` on the v2 fixture package before feature lowering begins.

Targets:

- Small v2 fixture package checks in under 2 seconds excluding interpreter startup.
- Synthetic 25-transform v2 fixture exposes no obvious repeated full-project scans.

## Risks

- Feature teams may implement separate operation metadata shapes if this sprint does not settle the foundation first.
- Cardinality can become confusing if joins, aggregations, and windows use different terms.
- Fixture examples can become too large to review if they try to demonstrate every edge case.

## Notes

Keep this sprint documentation-heavy and deliberately thin on feature lowering. The goal is a stable runway for v2, not
an implementation bundle that hides design decisions inside the first feature.
