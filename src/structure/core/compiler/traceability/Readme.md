# Compiler Traceability App

## Purpose
The compiler traceability app explains how source-level transform pieces become IR, target recipes, and generated
artifacts. Its output is compiler metadata for provenance and static dataflow, not runtime telemetry.

## Dependency Exchanges
The app consumes `PySparkExecutionPlan` recipes and their source links, then returns `CompilerTraceability` with
`CompilerProvenance`, `DataflowDependency`, and `OpaqueBoundary` records. The PySpark target uses it when rendering
traceability files, and CLI explain uses it to summarize static dataflow and hook boundaries.

## Inner Workings
`BuildCompilerTraceability` walks inputs, steps, hooks, validations, and outputs in target recipe order. It records
where generated nodes came from, how source columns flow into result columns, and where hooks form opaque boundaries
that Structure deliberately cannot inspect.
