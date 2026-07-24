# PySpark Compiler App

## Purpose
Lowers Core analysis plus a PySpark-authored body into the opaque `PySparkExecutionPlan` recipe graph. It also
classifies streaming compatibility and builds PySpark traceability.

## Dependency Exchanges
The app receives neutral `TransformPlan` analysis, PySpark authoring bodies, and capabilities. It returns recipes,
streaming reports, and traceability through `PySpark.compiler`; renderer and execution peers consume those contracts
through their own endpoints.

## Inner Workings
`lower()` delegates recipe conversion to `logic/maps/`, organized by steps and their operation, join, and aggregate
records. `streaming()` delegates compatibility checks to `logic/streaming/`; `traceability()` delegates provenance,
dataflow, and opaque-boundary construction to `logic/traceability/`.
