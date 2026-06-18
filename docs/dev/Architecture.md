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
src/pipeline_src/
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

generated/structure_generated/
  pipeline_src/pyspark/
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

Compiler phases must not depend on a live Spark installation. Discovery, schema extraction, symbolic execution,
compileability checks, IR construction, code generation, lineage generation, and generated-file diff checks run without
PySpark imports, Java, a SparkSession, or a Spark cluster. Generated PySpark may depend on PySpark at runtime; the
compiler itself must not.

The v1 default target is `target_pyspark = ">=3.5,<4.1"`, covering PySpark 3.5.x and 4.0.x. The emitter should prefer
the oldest clear optimizer-visible API inside the configured range.

Spark Connect belongs to v3 unless it can be supported through this emitter boundary without changing public DSL syntax,
generated class construction, or `run(...)` signatures.

## Compile-Time Performance

Compile-time performance is a product metric.

The compiler should avoid Spark dependencies during normal `check` and `compile`. It should use incremental
fingerprints, cache discovered models, and only regenerate files whose content changes.
