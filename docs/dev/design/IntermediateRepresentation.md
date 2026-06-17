# Design: Intermediate Representation

## Purpose

The IR separates Structure source semantics from PySpark code generation.

The compiler produces IR. Emitters consume IR.

## Core v1 IR

```text
TransformPlan
  name
  source_class
  generated_class
  inputs
  steps
  output_schema
  validation_policy
  lineage_events

StepPlan
  name
  input_schema
  output_schema
  operations
  hooks_before
  hooks_after
  validate_output

Operation
  Filter
  Project
  Join
  HookCall
  ValidateSchema
```

## Expression IR

```text
Expr
  FieldRef
  Literal
  CallExpr
  BinaryExpr
  BooleanExpr
  CastExpr
  WhenExpr
```

## v2 IR Extensions

- Aggregate
- GroupingSets
- Rollup
- Cube
- WindowProject
- HigherOrderFunctionExpr
- CacheHint
- JoinStrategyHint

## v3 IR Extensions

- ReadStream
- WriteStream
- Watermark
- Trigger
- Checkpoint
- StreamingStatePolicy

## Data Flow

```text
Symbolic execution
  ↓
StepPlan IR
  ↓ checks
TransformPlan IR
  ↓ emitters
Generated PySpark + lineage
```

## Compile-Time Performance

IR objects should be immutable or treated as immutable after construction. This enables caching, hashing, and incremental compile comparisons.
