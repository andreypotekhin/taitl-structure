# Structure Project Management Docs

This archive contains sprint-oriented project management documentation for the first implementation iterations of
**Structure**, a schema-first Python DSL and runtime/compiler toolkit that runs or generates clean PySpark DataFrame
code.

The sprint plan assumes the documentation set from the Structure design package already exists, especially:

- [UserStories.md](../specifications/UserStories.md)
- [GeneratedPySpark.md](../../GeneratedPySpark.md)
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
  V3.md
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
    Sprint11_V3DslAndSqlFunctionParity.md
    Sprint12_V3JoinParityHardening.md
    Sprint13_V3AggregationParity.md
    Sprint14_V3WindowParity.md
    Sprint15_V3CollectionHelperParity.md
    Sprint16_V3StreamingOrchestration.md
    Sprint17_V3IncrementalCompileCacheDiagnostics.md
    done/
      Sprint08_AggregationsWindowsHigherOrderFunctions.md
      Sprint09_OptimizationExplainDocsTooling.md
      Sprint10_DocsTestingIncrementalCompile.md
  templates/
    SprintReview.md
    ADR.md
    BugReport.md
```

## Pre-Coding Spike Gate

Sprint 00 includes a short spike gate before implementation of the first vertical slice. These spikes close the highest-risk design questions from [Challenges.md](../design/Challenges.md):

- `@after(method, lane=lane)` binding inside class bodies.
- Class-local `@special(type="expr")` helpers callable through `self` without a `self` parameter.
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
6. **Joins, compiler traceability, build integration**: `lookup_join`, N-step serial joins, compiler provenance, static
   dataflow traceability, `--fail-on-diff`.

V2 starts after v.1 stabilization evidence is release-ready. The v.2 sprint sequence is:

1. **Sprint 06: v.2 scope and analytical IR foundations**: release boundary, user stories, epics, IR operation taxonomy,
   capability checks, fixture skeletons, and diagnostic anchors.
2. **Sprint 07: analytical join coverage**: semi/anti existence joins, `inner_join(...)`, deterministic lookup dedupe,
   temporal validity-window joins, and backward as-of joins.
3. **Sprint 08: aggregations, windows, and higher-order functions**: typed `group_by(...)`, aggregate helpers,
   windowing, dedupe helpers, and compiler-visible array/map helpers.
4. **Sprint 09: Spark Connect, Spark streaming, optimization, and explain**: supported Spark Connect batch execution,
   static caller-owned Spark streaming compatibility, full rowset joins, advanced analytical operations, cache/persist
   first-slice directives, compact explain output, and explicit follow-up deferrals.
5. **Sprint 10: docs and testing**: generated documentation artifacts and pytest helpers.

V3 starts with the planned PySpark parity gaps tracked in [Gaps.md](../Gaps.md), then takes ownership of full streaming
orchestration. The v.3 sprint sequence is:

1. **Sprint 11: DSL and SQL function parity**: planned Column API and SQL function gaps.
2. **Sprint 12: join parity hardening**: using-key joins, diagnostics, cross safety, strategy directives, and forward
   as-of joins.
3. **Sprint 13: aggregation parity**: grouping sets and `having(...)`.
4. **Sprint 14: window parity**: null ordering, multiple order keys, and aggregate windows.
5. **Sprint 15: collection helper parity**: collection size/membership, map-key membership,
   array construction/repeat/union/except, element lookup, safe element lookup, and map concatenation.
6. **Sprint 16: streaming orchestration**: source/sink declarations, generated `readStream`/`writeStream`, triggers,
   checkpoints, output modes, watermarks, and state policies.
7. **Sprint 17: incremental compile and cache diagnostics**: `compile --changed-only`, cache invalidation, cache
   diagnostics, and warm compile performance fixtures.

V4 is reserved for backend expansion and any non-batch Spark Connect hardening left outside the Sprint 09 support
claim.
