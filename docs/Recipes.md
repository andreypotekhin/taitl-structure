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

More recipes should cover one recognizable outcome, make their data assumptions explicit, and link to the API or
reference pages that define their behavior. They should use ordinary Structure source rather than hand-written PySpark
unless the recipe is specifically about a hook.
