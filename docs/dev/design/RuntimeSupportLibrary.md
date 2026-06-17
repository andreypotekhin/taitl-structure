# Design: Runtime Support Library

## Purpose

The runtime library supports generated PySpark code with minimal functionality.

## Responsibilities

- `assert_schema(df, schema, name, mode)`
- `project_schema(df, schema)`
- `PipelineContext`
- schema comparison helpers

## Non-Goals

The runtime should not:

- discover transforms
- compile source files
- know compiler IR internals
- start Spark sessions
- manage Airflow or streaming lifecycle

## Schema Validation

Validation modes:

- `strict`
- `allow_extra_columns`
- `project_expected`

Default generated behavior:

```text
input validation: strict
intermediate validation: strict
final validation: strict
```

Hooks can request looser validation followed by projection.

## Data Flow

```text
generated transform
  ↓ assert_schema
DataFrame operations
  ↓ assert_schema
optional project_schema
  ↓ final DataFrame
```

## Compile-Time Performance

Runtime library does not affect compile time except for generated import paths. Keep it small and stable.
