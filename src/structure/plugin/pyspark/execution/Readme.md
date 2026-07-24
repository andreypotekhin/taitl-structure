# Direct Execution App

## Purpose
Interprets PySpark target recipes at runtime. This is Structure's default execution
path. It keeps transforms executable without need to write generated source files.

## Dependency Exchanges
The app consumes a source `Transform`, Spark session, optional context, input DataFrames, and a
`PySparkExecutionPlan`. It materializes schemas through PySpark target, applies filters, joins, projections, hooks
and validations, then returns a DataFrame or `TransformResult`.

## Inner Workings
`RunOnlinePySparkTransform` orchestrates `PySparkInputRecipe`, `PySparkStepRecipe`, and `PySparkOutputRecipe` objects
in order. Its implementation is grouped beneath `logic/` by expressions, steps, hooks, joins, aggregates, and outputs;
those actions render recipe intent as live PySpark `Column` and `DataFrame` operations, preserve `HookInputs` for
original-input hooks, and enforce schema checks at the same recipe points generated code would render them.

## Generated execution
`RunGeneratedPySparkTransform` imports the generated transform module, checks its semantic fingerprint, invokes its
generated class with the bound inputs, and normalizes the result to `TransformResult`. This preserves the same
invocation contract while making checked-in generated source the operational artifact.
