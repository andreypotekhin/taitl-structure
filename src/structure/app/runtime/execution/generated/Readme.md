# Generated Execution App

## Purpose
The generated execution app runs checked-in generated PySpark classes for a bound Structure transform. It supports
teams that want generated source as the operational artifact while preserving the same invocation model as online mode.

## Dependency Exchanges
The app consumes a bound source `Transform`, generated package name, `PySparkExecutionPlan` metadata, session Spark
and context objects, and named input DataFrames. It imports the generated module and class, invokes `run(...)`, and
returns either a single DataFrame or a `TransformResult`.

## Inner Workings
`RunGeneratedPySparkTransform` derives the generated module and class name from the source transform and configured
package, constructs the generated class with `spark` and `ctx`, passes bound inputs by name, and wraps import or
contract failures in `StructureRuntimeError` diagnostics.
