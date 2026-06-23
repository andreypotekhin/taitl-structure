# Structure Project Management Docs

This archive contains sprint-oriented project management documentation for the first implementation iterations of
**Structure**, a schema-first Python DSL and runtime/compiler toolkit that runs or generates clean PySpark DataFrame
code.

The sprint plan assumes the documentation set from the Structure design package already exists, especially:

- `docs/specifications/UserStories.md`
- `docs/GeneratedPySpark.md`
- `devdocs/Architecture.md`
- `devdocs/Implementation.md`
- `devdocs/Testing.md`
- `devdocs/design/*.md`

## Contents

```text
project-management/
  Roadmap.md
  Milestones.md
  Backlog.md
  DefinitionOfDone.md
  SprintPlanningGuide.md
  TraceabilityMatrix.md
  RisksAndMitigations.md
  sprints/
    Sprint00_Groundwork.md
    Sprint01_VerticalSlice1.md
    Sprint02_SchemasAndValidation.md
    Sprint03_SymbolicExpressionsFilteringHelpers.md
    Sprint04_HooksAndGeneratedClasses.md
    Sprint05_JoinsTraceabilityBuildIntegration.md
    Sprint06_V2ScopeAndAnalyticalIR.md
    Sprint07_AnalyticalJoinCoverage.md
    Sprint08_AggregationsWindowsHigherOrderFunctions.md
    Sprint09_OptimizationExplainDocsTooling.md
  templates/
    SprintReview.md
    ADR.md
    BugReport.md
```

## Pre-Coding Spike Gate

Sprint 00 includes a short spike gate before implementation of the first vertical slice. These spikes close the highest-risk design questions from `docs/dev/design/Challenges.md`:

- `@after(method)` binding inside class bodies.
- Class-local `@expr_fn` helpers callable through `self` without a `self` parameter.
- Source-order discovery with stable line numbers.
- Source-root discovery and generated `structure_generated.<source package>` import paths.
- `StructureSession` and deferred transform invocation API.
- Compiler checks that do not import PySpark or start Spark.
- A minimal generated PySpark execution test using local Spark.

Sprint 01 should not start until the spike notes are captured and any resulting design changes are reflected in the sprint scope.

## Iteration Strategy

The first iterations are intentionally arranged to build confidence in small compiler slices:

1. **Groundwork and spikes**: repository, package layout, config, CLI skeleton, testing harness, and pre-coding proofs.
2. **Vertical Slice 1**: one input schema, one transform method, online execution, optional generated PySpark class,
   one Spark execution test.
3. **Schemas and validation**: richer schema model, `StructType` generation, input/intermediate/output validation.
4. **Symbolic expressions, filtering, helpers**: compiler-worthy expression model with strict unsupported-code diagnostics.
5. **Hooks and generated classes**: source hooks, clean no-hook generated code, direct hook calls.
6. **Joins, compiler traceability, build integration**: `join_one`, N-step serial joins, compiler provenance, static
   dataflow traceability, `--fail-on-diff`.

V2 starts after v1 stabilization evidence is release-ready. The v2 sprint sequence is:

1. **Sprint 06: v2 scope and analytical IR foundations**: release boundary, user stories, epics, IR operation taxonomy,
   capability checks, fixture skeletons, and diagnostic anchors.
2. **Sprint 07: analytical join coverage**: semi/anti existence joins, `join_many(...)`, deterministic lookup dedupe,
   temporal validity-window joins, and backward as-of joins.
3. **Sprint 08: aggregations, windows, and higher-order functions**: typed `group_by(...)`, aggregate helpers,
   windowing, dedupe helpers, and compiler-visible array/map helpers.
4. **Sprint 09: optimization, explain, docs, and test tooling**: explicit optimization directives, richer explain
   output, generated documentation artifacts, pytest helpers, and production incremental compile.

V3 is reserved for streaming orchestration, and V4 is reserved for Spark Connect backend expansion.
