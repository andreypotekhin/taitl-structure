# Compiler Frontend App

## Purpose
The compiler frontend app turns a decorated `Transform` class into backend-neutral `TransformPlan` IR. It is the
source-aware phase that validates transform class shape, method order, input and output lanes, hooks, and projected
schema assignments.

## Dependency Exchanges
The app consumes DSL classes such as `Transform`, `Structure`, `OutputDeclaration`, `RowScope`, and expression
helpers, records symbolic effects through `CompileContext`, and emits `TransformPlan`, `InputPlan`, `StepPlan`,
`OutputPlan`, `HookPlan`, and `ProjectAssignment` objects. It raises `StructureCompileError` with registry-backed
diagnostics for invalid source.

## Inner Workings
`CompileTransform` is the central action. It instantiates the transform class, scans public schema-returning methods in
source order, runs each method inside a symbolic `CompileContext`, builds projection assignments from returned schema
instances, attaches before/after hooks, and resolves final outputs from default or explicit lanes.
