# PySpark Symbolic Execution App

## Purpose
Opens the PySpark-owned step capture context used while Core invokes a transform method symbolically. It records
symbolic relations and operations into a PySpark body.

## Dependency Exchanges
The authoring facet opens a context through `PySpark.symbolic_execution.open()`. DSL scopes and operation helpers use
the installed public symbolic-context contract; compiler receives the captured body through the authoring session.

## Inner Workings
`OpenPySparkStep` creates an isolated context backed by the shared context variable. Result construction belongs in
`logic/results/`; as the symbolic body grows, projection construction, aggregate construction, and validation rules
live beside it under their respective domain packages.
