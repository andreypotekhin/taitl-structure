# Online Execution App

## Purpose
The online execution app interprets PySpark target recipes directly at runtime. It is Structure's default execution
path and keeps authored transforms executable without first writing generated source files.

## Dependency Exchanges
The app consumes a bound source `Transform`, live Spark session, optional context, input DataFrames, and a
`PySparkExecutionPlan`. It materializes schemas through the PySpark target, applies filters, joins, projections, hooks,
and validations, then returns a DataFrame or `TransformResult`.

## Inner Workings
`RunOnlinePySparkTransform` walks `PySparkInputRecipe`, `PySparkStepRecipe`, and `PySparkOutputRecipe` objects in order.
It renders recipe intent as live PySpark `Column` and `DataFrame` operations, uses `HookInputs` when hooks request
original inputs, and enforces schema checks at the same recipe points generated code would render them.
