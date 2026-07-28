# Structure Project Management Docs

This archive contains sprint-oriented project management documentation for the first implementation iterations of
**Structure**, a schema-first Python DSL and runtime/compiler toolkit that runs or generates clean PySpark DataFrame
code.

The sprint plan assumes the documentation set from the Structure design package already exists, especially:

- [UserStories.md](../specifications/UserStories.md)
- [GeneratedSource.md](../../GeneratedSource.md)
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
    Sprint15_V3CollectionHelperParity.md
    Sprint16_V3StreamingOrchestration.md
    Sprint18_V4StreamingMigration.md
    Sprint28_V7ScopeAndDesign.md
    Sprint29_V7GeneratorExpansionAndDelegates.md
    Sprint30_V7BinaryEncoding.md
    Sprint31_V7SchemaCarryingParsing.md
    Sprint32_V7DeterministicMode.md
    Sprint33_V7StreamStaticEnrichment.md
    Sprint34_V7StreamStaticOuterLookup.md
    Sprint35_V7SingleStatefulStreamingComposition.md
    done/
      Sprint27_V6ReleaseAndChallengeClosure.md
      Sprint26_V6OrderedTimelineRecurrence.md
      Sprint25_V6RelationOperationsAndSearchMigration.md
      Sprint24_V6SecurityReconciliation.md
      Sprint23_V6ApiLedgerAndPluginDecomposition.md
      Sprint12_V3JoinParityHardening.md
      Sprint13_V3AggregationParity.md
      Sprint14_V3WindowParity.md
      Sprint17_V4TransformationApiCoverage.md
      SprintV4_Hardening.md
      Sprint08_AggregationsWindowsHigherOrderFunctions.md
      Sprint11_V3DslAndSqlFunctionParity.md
      Sprint09_OptimizationExplainDocsTooling.md
      Sprint10_DocsTestingIncrementalCompile.md
  templates/
    SprintReview.md
    ADR.md
    BugReport.md
```

## Pre-Coding Spike Gate

Sprint 00 includes a short spike gate before implementation of the first vertical slice. These spikes close the highest-risk design questions from [Challenges.md](../design/Challenges.md):

- `@raw(lane=lane)` binding inside class bodies.
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

V2 starts after v1 stabilization evidence is release-ready. The v2 sprint sequence is:

1. **Sprint 06: v2 scope and analytical IR foundations**: release boundary, user stories, epics, IR operation taxonomy,
   capability checks, fixture skeletons, and diagnostic anchors.
2. **Sprint 07: analytical join coverage**: semi/anti existence joins, `inner_join(...)`, deterministic lookup dedupe,
   temporal validity-window joins, and backward as-of joins.
3. **Sprint 08: aggregations, windows, and higher-order functions**: typed `group_by(...)`, aggregate helpers,
   windowing, dedupe helpers, and compiler-visible array/map helpers.
4. **Sprint 09: Spark Connect, Spark streaming, optimization, and explain**: supported Spark Connect batch execution,
   static caller-owned Spark streaming compatibility, full rowset joins, advanced analytical operations, cache/persist
   first-slice directives, compact explain output, and explicit follow-up deferrals.
5. **Sprint 10: docs and testing**: generated documentation artifacts and pytest helpers.

V3 closed its scheduled PySpark parity gaps and hardened compiler-visible streaming transformations while keeping
streaming lifecycle ownership with callers. The completed v3 sprint sequence is:

1. **Sprint 11: DSL and SQL function parity**: planned Column API and SQL function gaps.
2. **Sprint 12: join parity hardening**: using-key joins, diagnostics, cross safety, strategy directives, and forward
   as-of joins.
3. **Sprint 13: aggregation parity**: grouping sets and `having(...)`.
4. **Sprint 14: window parity**: null ordering, multiple order keys, and aggregate windows.
5. **Sprint 15: collection helper parity**: collection size/membership, map-key membership,
   array construction/repeat/union/except, element lookup, safe element lookup, and map concatenation.
6. **Sprint 16: streaming transformation hardening**: watermarks, admitted state policies, diagnostics, public
   examples, and caller-owned file-stream evidence.
V4 begins with **Sprint 17: transformation API coverage foundation**, then expands predictable PySpark transformation
coverage across expressions, nested values, relational operations, joins, aggregations, windows, and collections.
Loading, storage, and orchestration remain caller-owned.

## Version Hardening Cadence

Every version ends with a dedicated hardening sprint after its feature-delivery sprints. The sprint admits no new
feature scope; it resolves release blockers and collects release evidence for regression, parity, compatibility,
generated artifacts, documentation, diagnostics, and performance baselines.

v4 closed with the [Final v4 Hardening Sprint](sprints/done/SprintV4_Hardening.md), after all of its feature sprints
and before v5 implementation resumed.

v6 follows the v5 plugin hardening work with a deliberately bounded typed-PySpark closure program:

1. **Sprint 23: API ledger and plugin decomposition**: classify the remaining API frontier, characterize behavior,
   and extract the large PySpark modules without a semantic change.
2. **Sprint 24: compiler-visible Security reconciliation**: typed lambda struct fields, analytic maximum, ordered
   collection/exact-one/global aggregates, and retirement of both Security raw hooks.
3. **Sprint 25 (+): typed relation operations and Search migration**: generators, union composition, self aliases,
   ordering/selection, and hook retirement after output-equivalence evidence.
4. **Sprint 26 (+): bounded timeline recurrence**: the separately specified batch-only ordered `scan(...)` feature.
5. **Sprint 27 (active): release and challenge closure**: live evidence, documentation, executable specifications, and C27--C34
   disposition, with no new feature family.
