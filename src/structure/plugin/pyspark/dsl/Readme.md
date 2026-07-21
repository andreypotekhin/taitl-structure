# PySpark DSL App

## Purpose
Owns every PySpark-specific authoring concept: field factories, concrete types, expressions, joins, operations,
aggregation, windows, and authored body records.

## Dependency Exchanges
Authors import the supported vocabulary from `structure.plugin.pyspark`. Compiler and schema apps consume the public
DSL models, while the DSL records symbolic effects through the plugin symbolic-execution contract rather than
calling another app's private implementation.

## Inner Workings
Implementation families are grouped by concern beneath this package. The top-level plugin package exports the
author-facing DSL without requiring a `dsl.` prefix.
