# Recipes

Recipes are focused, end-to-end guides for a common pipeline outcome. Each starts with a small business problem,
shows the schemas and transform that solve it, and calls out the operational choices that make the result safe in a
real pipeline.

Use a recipe after [Getting Started](GettingStarted.md) when you know the outcome you need but want more context than a
quick-reference snippet. Recipes complement the [Quick Reference](QuickRef.md), which remains the best place to scan
the API, and the [Reference](Reference.md), which defines detailed behavior.

## Selection Recipes

- [Latest Rows](recipes/LatestRows.md): retain the most recent row for each business key.
- [Earliest Rows](recipes/EarliestRows.md): retain the first row for each business key.

## Analytical State Recipes

- [Sketch and Bitmap Metrics](recipes/SketchBitmapMetrics.md): publish reusable typed HLL/Bitmap state and later
  derive estimates or counts without treating the state as generic Binary data.

## Source Layout Recipes

- [Colocated Intermediate Schemas](recipes/ColocatedIntermediateSchemas.md): keep a transform-only schema beside its transform.

## Integration Boundary Recipes

- [Geospatial Provider Bridge](recipes/GeospatialProviderBridge.md): make a provider-specific Binary handoff explicit.
- [Delta Table Mutations](recipes/DeltaTableMutations.md): mutate a caller-owned table from a typed transform.
- [Delta Schema Evolution](recipes/DeltaSchemaEvolution.md): declare and check a table's expected schema transition.
- [Delta Change Data Feed](recipes/DeltaChangeDataFeed.md): read batch CDF through a typed transform.
- [Delta Inspection and Maintenance](recipes/DeltaInspectionAndMaintenance.md): inspect history and perform restore, optimize, or vacuum effects.

More recipes should cover one recognizable outcome, make their data assumptions explicit, and link to the API or
reference pages that define their behavior. They should use ordinary Structure source rather than hand-written PySpark
unless the recipe is specifically about a hook.
