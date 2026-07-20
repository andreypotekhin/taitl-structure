# PySpark Schema App

## Purpose
Materializes, validates, reads, renders, and source-maps PySpark schemas. Concrete type semantics remain in the
PySpark DSL.

## Dependency Exchanges
The app consumes Structure schema declarations and PySpark DSL field types. It returns PySpark schema values, rendered
source, and transform schema sets through `PySpark.schema`; compiler, renderer, and execution peers use those endpoint
commands.

## Inner Workings
Separate commands validate plugin-owned fields, materialize runtime schema objects, read Spark schema objects, render
source, and assemble transform input/output schema contracts.
