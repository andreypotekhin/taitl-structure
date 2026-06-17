# Structure Project Management Docs

This archive contains sprint-oriented project management documentation for the first implementation iterations of **Structure**, a schema-first Python DSL and compiler that generates clean PySpark DataFrame code.

The sprint plan assumes the documentation set from the Structure design package already exists, especially:

- `docs/Specification.md`
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
    Sprint05_JoinsLineageBuildIntegration.md
  templates/
    SprintReview.md
    ADR.md
    BugReport.md
```

## Iteration Strategy

The first iterations are intentionally arranged to build confidence in small compiler slices:

1. **Groundwork**: repository, package layout, config, CLI skeleton, testing harness.
2. **Vertical Slice 1**: one input schema, one transform method, one generated PySpark class, one Spark execution test.
3. **Schemas and validation**: richer schema model, `StructType` generation, input/intermediate/output validation.
4. **Symbolic expressions, filtering, helpers**: compiler-worthy expression model with strict unsupported-code diagnostics.
5. **Hooks and generated classes**: source hooks, clean no-hook generated code, direct hook calls.
6. **Joins, lineage, build integration**: `join_one`, N-step serial joins, LDJSON lineage, `--fail-on-diff`.

V2 features such as aggregations, windowing, HOFs, advanced grouping, caching, and join strategy optimization are captured in the backlog but not scheduled in the first v1 implementation sprints.
