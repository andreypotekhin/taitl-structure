# Compiler Frontend App

## Purpose
The compiler frontend separates Structure analysis from platform compilation. `analyze()` produces a structural
`TransformPlan` without evaluating a step body. `compile()` selects a platform, has it author opaque step bodies while
Core preserves Structure lifecycle rules, then dispatches the completed plan to that platform compiler.

## Dependency Exchanges
The app consumes `Transform` declarations, inheritance metadata, inputs, lanes, outputs, decorators, and hooks. It
emits structural `TransformPlan`, `InputPlan`, `StepPlan`, `StepResultPlan`, `OutputPlan`, and `HookPlan` values.
During compilation it exchanges `StepAuthoringRequest` and opaque `StepAuthoringCapture` values with the selected
platform. PySpark expressions, projections, joins, aggregates, concrete types, and target diagnostics belong to the
bundled PySpark plugin rather than this app.

## Inner Workings
`AnalyzeTransform` discovers declarations, validates inheritance and lane routing, attaches hooks at their structural
position, and resolves final outputs. `CompilePlatformTransform` negotiates the platform, validates declarations,
invokes steps in Core-defined source order with platform-supplied arguments, and calls the selected compiler facet once.
The bundled documentation workflow uses the frontend authoring endpoint to retain PySpark-rich educational output.
