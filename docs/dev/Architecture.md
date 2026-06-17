# Architecture

Structure is a compiler and code generator for schema-driven PySpark data pipelines.

It is intentionally not a heavy runtime framework. The source DSL compiles to generated PySpark classes, and generated code performs the actual DataFrame transformations.

## Goals

- Keep user-facing transforms schema-first and IDE-friendly.
- Generate Spark optimizer-visible DataFrame and Column expressions.
- Reject unsupported Python code rather than silently creating UDFs.
- Produce clean, deterministic, reviewable PySpark.
- Support explicit arbitrary PySpark hooks.
- Keep generated code usable from Airflow and ordinary Spark jobs.
- Track PySpark evolution through a narrow backend emitter layer.
- Support seed config defaults while keeping user configuration small.

## High-Level Flow

```text
structure/src/
  schemas/
  transforms/

        ↓ structure compile

compiler
  discovery
  symbolic execution
  checks
  IR construction
  code generation
  lineage generation

        ↓

structure/generated/
  schemas/
  transforms/
  runtime/
  lineage/

        ↓

Airflow / Spark job imports generated code
```

## Major Components

### Public DSL

Package:

```text
structure/dsl/
```

Responsibilities:

- `@transform`
- `input(...)`
- `@expr_fn`
- `where(...)`
- `@before(method)`
- `@after(method)`
- schema fields and types
- symbolic expression functions
- join declarations
- validation decorators and enums
- v2 optimization hints

The DSL should be lightweight and mostly independent of PySpark.

### Configuration

Configuration is convention-first.

Default seed config:

```toml
[tool.structure]
source_dir = "structure/src"
generated_dir = "structure/generated"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.2"
validate_inputs = true
validate_intermediate = true
validate_outputs = true
lineage = "basic"
streaming_compatibility_checks = true
strict_performance = true
format_generated = true
fail_on_diff = false
```

User config should only specify settings that differ from defaults.

Resolution order:

1. CLI flags.
2. `pyproject.toml` `[tool.structure]`.
3. `structure.toml`.
4. seed defaults.

### Discovery

Package:

```text
structure/compiler/discovery.py
```

Responsibilities:

- read project settings
- scan source directories
- import source modules
- find `@transform` classes
- preserve source order of class members
- identify inputs, expression helpers, hooks, and schema-returning methods

### Symbolic Execution

Package:

```text
structure/compiler/symbolic/
```

Responsibilities:

- create symbolic schema proxies
- execute subtransform methods against symbolic values
- collect field references, expressions, filters, joins, and projections
- detect unsupported operations
- build structured diagnostics

### Intermediate Representation

Package:

```text
structure/compiler/ir/
```

The IR separates source DSL semantics from generated PySpark code.

Core v1 IR nodes:

- `TransformPlan`
- `StepPlan`
- `Project`
- `Filter`
- `Join`
- `HookCall`
- `ValidateSchema`
- `SchemaDef`
- `FieldDef`
- `Expr`

V2 IR nodes:

- `Aggregate`
- `WindowProject`
- `HigherOrderFunctionExpr`
- `CacheHint`
- `PersistHint`
- `JoinStrategyHint`
- `AdvancedGrouping`
- `LineageEvent`

V3 IR nodes:

- `ReadStream`
- `WriteStream`
- `Watermark`
- `Trigger`
- `Checkpoint`
- `StreamingStatePolicy`

### Checkers

Package:

```text
structure/compiler/checks/
```

Responsibilities:

- schema field existence
- type compatibility
- source-order type flow
- hook signature validation
- join condition validation
- streaming compatibility checks
- performance guardrail checks
- config workaround hints for diagnostics

### PySpark Emitter

Package:

```text
structure/generator/pyspark/
```

Responsibilities:

- generate Spark `StructType` modules
- generate transform classes
- generate convenience functions
- generate runtime imports
- generate hook calls only when hooks exist
- generate LDJSON lineage
- select PySpark-version-compatible code patterns

The compiler should not directly write PySpark expressions. It should produce IR. The PySpark emitter should lower IR to code.

### Runtime Support

Package:

```text
structure/runtime/
```

or copied/generated into:

```text
structure/generated/runtime/
```

Responsibilities:

- `assert_schema(...)`
- `project_schema(...)`
- `PipelineContext`
- schema comparison helpers

Runtime should stay small and independent of compiler internals.

## Generated Code Shape

Each source transform class maps to one generated class.

```python
class EnrichOrdersGenerated:

    def __init__(self, *, spark, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()  # only if hooks exist

    def run(self, *, orders, customers, products):
        ...
```

If no hooks exist, generated code omits the source transform import and `_impl`.

## PySpark Evolution Strategy

Structure should isolate PySpark API usage in the PySpark backend emitter.

Recommended layers:

```text
compiler IR
  ↓
backend-neutral emitter interface
  ↓
PySpark emitter
  ↓
generated code
```

The emitter owns:

- function names
- version compatibility
- import strategy
- aliasing patterns
- schema code generation
- join hint generation
- future PySpark API differences

User DSL code should remain stable across PySpark versions.

## Streaming Compatibility

V1 and v2 do not generate streaming orchestration. They generate transforms that operate on DataFrames.

If a caller passes a streaming DataFrame and the generated operations are streaming-compatible, Spark Structured Streaming owns execution.

Structure may validate streaming compatibility, but it does not generate:

- `readStream`
- `writeStream`
- triggers
- checkpointing
- streaming job lifecycle

Those belong to v3.

## Lineage Architecture

Lineage should be generated from IR, not extracted from generated code.

Default lineage format: LDJSON.

Default lineage level: `basic`.

Basic lineage events:

- transform
- input
- step
- join
- hook
- output

Optional `fields` lineage adds output-field dependencies.

Debug lineage may include full expression trees and source locations.
