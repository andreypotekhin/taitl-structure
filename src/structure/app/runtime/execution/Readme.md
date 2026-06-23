# Runtime Execution App

## Purpose
The runtime execution app groups concrete transform runners. It separates how a prepared plan is executed from how
sessions are configured and how schemas are materialized.

## Dependency Exchanges
The app receives bound transforms, `StructureSession` state, `PySparkExecutionPlan` recipes, and input DataFrames
from the session app. Its nested runners return DataFrames or `TransformResult` values and raise runtime diagnostics
when a configured execution path cannot satisfy the transform invocation.

## Inner Workings
The two nested execution modes mirror Structure's product contract. `online` interprets target recipes directly
against live PySpark objects, while `generated` imports the generated transform class and delegates execution to its
`run(...)` method.
