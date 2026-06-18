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
src/orders/
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
  compiler provenance
  static dataflow lineage

        ↓

generated/structure_generated/
  orders/pyspark/
    schemas/
    transforms/
  runtime/
  lineage/  # compiler metadata, not runtime telemetry

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

Compiler phases must not depend on a live Spark installation. Discovery, schema extraction, symbolic execution,
compileability checks, IR construction, code generation, compiler provenance, static dataflow lineage, and
generated-file diff checks run without PySpark imports, Java, a SparkSession, or a Spark cluster. Generated PySpark may
depend on PySpark at runtime; the compiler itself must not.

The v1 default target is `target_pyspark = ">=3.5,<4.1"`, covering PySpark 3.5.x and 4.0.x. The emitter should prefer
the oldest clear optimizer-visible API inside the configured range.

Spark Connect belongs to v4 unless it can be supported through this emitter boundary without changing public DSL syntax,
generated class construction, `run(...)` signatures, or streaming orchestration semantics.

## Compile-Time Performance

Compile-time performance is a product metric.

The compiler should avoid Spark dependencies during normal `check` and `compile`. v1 should preserve deterministic
outputs and source fingerprints so production incremental compilation can arrive in v2 without reshaping the compiler.
