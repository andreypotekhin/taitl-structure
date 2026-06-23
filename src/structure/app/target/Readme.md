# Target App

## Purpose
The target app owns concrete backend semantics below the backend-neutral compiler IR. It is the boundary that prevents
PySpark version concerns, code generation details, and execution recipes from leaking into the DSL or compiler
frontend.

## Dependency Exchanges
The app consumes compiler IR, Structure schema/type metadata, configured backend requirements, and target version
ranges. It returns capability decisions, PySpark execution recipes, generated source files, generated-file diffs, and
runtime schema materializations for CLI, runtime, traceability, and tests.

## Inner Workings
Target is split into `capabilities` and concrete target apps such as `pyspark`. Capabilities answer whether a target
supports a feature; concrete target code lowers IR to recipes, renders source modules, writes or compares generated
files, and materializes runtime schemas.
