# Architecture

Structure is a compiler and code generator for schema-driven PySpark data pipelines.

It is not intended to be a heavy runtime framework. Source DSL files compile to generated PySpark classes. Generated code performs DataFrame transformations at runtime.

## Goals

- Schema-first authoring.
- IDE-friendly source code.
- Spark optimizer-visible generated code.
- Explicit arbitrary PySpark hooks.
- Clean hook-free generated code.
- Small runtime support library.
- Fast compiler feedback.
- Version-aware PySpark emitter.

## High-Level Data Flow

```text
structure/src/pipeline_src/
  schemas/
  transforms/

        ↓ structure compile

compiler
  config
  discovery
  symbolic execution
  compileability checks
  IR generation
  code generation
  lineage generation

        ↓

structure/generated/pipeline_generated/pyspark/
  schemas/
  transforms/
  runtime/
  lineage/

        ↓

Airflow / Spark job imports generated code
```

## Major Components

- DSL
- Schema model
- Discovery and inspection
- Symbolic execution engine
- Intermediate representation
- Compileability checker
- PySpark code generator
- Runtime support library
- CLI

Each component has a detailed design document under `devdocs/design/`.

## Backend Boundary

The compiler produces backend-neutral IR. The PySpark code generator lowers IR to concrete PySpark code.

This boundary is important for keeping up with PySpark evolution. PySpark API compatibility should be isolated in the PySpark emitter rather than scattered across discovery, symbolic execution, or checks.

## Compile-Time Performance

Compile-time performance is a product metric.

The compiler should avoid starting Spark during normal `check` and `compile`. It should use incremental fingerprints, cache discovered models, and only regenerate files whose content changes.
