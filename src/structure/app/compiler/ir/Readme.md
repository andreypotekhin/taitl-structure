# Compiler IR App

## Purpose
The compiler IR app defines Structure's backend-neutral transform model. It is the stable handoff from source
understanding to target lowering, execution, traceability, and compileability checks.

## Dependency Exchanges
The app receives schema classes, DSL expressions, hook options, joins, filters, and projection assignments from the
compiler frontend. It exports immutable-looking dataclass records such as `TransformPlan`, `StepPlan`, `InputPlan`,
`OutputPlan`, `JoinPlan`, `HookPlan`, and `ProjectAssignment` for target, runtime, traceability, and tests.

## Inner Workings
The model is intentionally data-heavy and behavior-light. `TransformPlan` owns transform-level inputs, ordered steps,
outputs, and options; `StepPlan` owns per-subtransform filters, joins, projections, hooks, and lane names; the smaller
plan records keep field-level, join-level, and hook-level details explicit for later passes.
