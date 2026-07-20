# PySpark Compiler App

## Purpose
Lowers Core analysis plus a PySpark-authored body into the opaque `PySparkExecutionPlan` recipe graph. It also
classifies streaming compatibility and builds PySpark traceability.

## Dependency Exchanges
The app receives neutral `TransformPlan` analysis, PySpark authoring bodies, and capabilities. It returns recipes,
streaming reports, and traceability through `PySpark.compiler`; renderer and execution peers consume those contracts
through their own endpoints.

## Inner Workings
`lower()` maps analysis and body records to recipe records. `streaming()` checks recipe compatibility, and
`traceability()` records the relation between source, analysis, recipes, and generated artifacts.
