# Runtime Session App

## Purpose
The runtime session app owns the public runtime orchestration object, `StructureSession`. It binds Spark, caller
context, execution mode, generated package settings, and target backend choices into one reusable execution context.

## Dependency Exchanges
The app consumes transform invocations from the DSL app, compiles them with the compiler frontend, lowers them through
the PySpark target, and delegates to online or generated execution apps. It exposes `StructureSession`,
`TransformResult`, `RuntimeDiagnostic`, and `StructureRuntimeError` through the runtime API.

## Inner Workings
`StructureSession.run(...)` validates that it received a bound `Transform`, builds a target execution plan, optionally
builds `TransformSchemas`, chooses the runner for `online` or `generated`, and returns the runner result.
`TransformResult` provides mapping behavior for multi-output transforms while preserving simple single-output returns.
