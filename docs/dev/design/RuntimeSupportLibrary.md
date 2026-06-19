# Design: Runtime Support Library

## Purpose

The runtime library supports online and generated PySpark execution with focused, reusable helpers.

## Responsibilities

- `assert_schema(df, schema, name, mode)`
- `project_schema(df, schema)`
- `PipelineContext`
- schema comparison helpers
- `StructureSession`
- runtime runner registry
- hook input namespace

## Non-Goals

The shared runtime helpers should not:

- own compiler frontend behavior
- start Spark sessions
- manage Airflow or streaming lifecycle

The online execution runtime may ask the compiler frontend for a `TransformPlan`, but that responsibility belongs to a
runner component rather than low-level schema helpers.

## Schema Validation

Validation modes:

- `strict`
- `allow_extra_columns`
- `project_expected`

Default runtime behavior:

```text
input validation: strict
intermediate validation: strict
final validation: strict
```

Hooks can request looser validation followed by projection.

## Data Flow

```text
online or generated transform
  -> assert_schema
DataFrame operations
  -> assert_schema
optional project_schema
  -> final DataFrame
```

## Compile-Time Performance

Runtime library does not affect compile time except for generated import paths and public API compatibility. Keep it
small and stable. Compiler commands must remain Spark-free even though online runtime execution may import PySpark.
