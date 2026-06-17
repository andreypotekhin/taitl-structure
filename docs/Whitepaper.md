# Whitepaper: Structure

## Typed Struct-to-Struct Transform Compilation for PySpark

## Abstract

Structure is an open-source Python DSL and code generator for building schema-enforced, IDE-friendly, Spark-optimized data pipelines. It lets developers describe data processing as typed schema-to-schema transformations while generating clean PySpark DataFrame code suitable for Airflow, Spark jobs, and batch data platforms.

Structure is designed for teams that want the maintainability of object-style schema transformations without giving up Spark's optimizer-friendly DataFrame execution model.

## Problem

Large-scale data pipelines are often written directly in PySpark DataFrame code, SQL, or table-oriented transformation frameworks. These approaches are powerful but can become difficult to maintain when business logic is naturally expressed as transformations between nested records, domain objects, or schemas.

Common pain points include:

- Weak schema enforcement across multi-step pipelines.
- Many transformations represented as column strings.
- Limited IDE navigation and refactoring support.
- Hard-to-review dynamically assembled DataFrame logic.
- Business logic hidden in arbitrary Python functions or UDFs.
- Airflow DAGs tied directly to verbose transformation code.
- Difficulty separating generated, compiler-checked logic from custom PySpark escape hatches.

## Design Rationale: Performance First

Structure focuses on generating Spark DataFrame and Column expressions rather than row-wise Python functions because Spark can optimize transformations only when the logical plan remains visible.

Projection, filtering, joins, predicate pushdown, column pruning, aggregation planning, join planning, and code generation all depend on expressing work in Spark's relational expression model. Structure therefore treats unsupported Python operations as compile errors rather than silently degrading to UDFs, row-wise maps, or local Python execution.

Arbitrary PySpark is still supported through explicit hooks, but the compiled path remains strict.

## Design Goals

1. Schema-first transformation design.
2. IDE-friendly authoring.
3. Spark-optimized execution.
4. Generated code visibility.
5. Explicit escape hatches.
6. Convention by default with small TOML configuration when needed.
7. Minimal string references.
8. Build and CI integration.
9. Streaming-compatible DataFrame transforms in v1/v2.
10. Compact lineage artifacts.

## Core Model

A Structure source transform is a decorated Python class.

```python
@transform
class EnrichOrders(Transform):

    orders = input(OrderRaw)
    customers = input(Customer)
    products = input(Product)

    @expr_fn
    def clean_id(value):
        return lower(trim(value))

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())

        return OrderNormalized(
            id=order.id,
            customer_id=self.clean_id(order.customer_id),
            product_id=self.clean_id(order.product_id),
            total=to_decimal(order.total, precision=12, scale=2),
        )
```

The compiler symbolically executes each schema-returning method and generates PySpark DataFrame code.

## Generated Code Model

Structure generates one class per source transform class.

```python
class EnrichOrdersGenerated:

    def __init__(self, *, spark, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()  # only if hooks exist

    def run(self, *, orders, customers, products):
        ...
```

A convenience function may also be generated.

## Unsupported Code Detection

Unsupported code detection is a performance feature, not merely a correctness feature. If arbitrary Python logic were silently accepted, the compiler would either have to generate Python UDFs, row-wise map operations, or opaque runtime callbacks. Those forms reduce Spark's ability to optimize execution, can prevent predicate and projection pushdown, and often introduce serialization overhead.

Structure rejects unsupported compiled-transform code so that generated output remains Spark-plan-visible by default.

Example unsupported source:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized(
        customer_id=order.customer_id.strip().lower(),
    )
```

Example error:

```text
CompileError: Unsupported expression in compiled subtransform

Transform:
  EnrichOrders

Subtransform:
  normalize

Output field:
  OrderNormalized.customer_id

Source expression:
  order.customer_id.strip().lower()

Problem:
  Python string methods .strip() and .lower() cannot be compiled to Spark Column expressions.

Why this matters:
  Compiled subtransforms must lower to Spark-plan-visible Column expressions.
  Silent fallback to UDFs would reduce optimizer visibility and add Python serialization overhead.

Use direct DSL functions:
  customer_id=lower(trim(order.customer_id))

For reuse:
  @expr_fn
  def clean_id(value):
      return lower(trim(value))

For arbitrary PySpark:
  @after(normalize)
  def clean_id_column(self, *, df, spark, ctx):
      return df.withColumn("customer_id", F.lower(F.trim(F.col("customer_id"))))

Config workaround:
  No configuration setting allows unsupported Python in compiled subtransforms.
  This is intentional to protect optimizer-visible execution.
```

If a configuration workaround exists for a particular error, Structure should include it. For example, intermediate schema mismatch errors can suggest `validate_intermediate = false` or a per-method validation override.

## Roadmap

### v1

Projection, filtering, joins, typed intermediate schemas, generated PySpark classes, hooks, schema validation, basic LDJSON lineage, streaming-compatible generated transforms, and build/CI integration.

### v2

Aggregations, windowing, deduplication helpers, higher-order functions for nested arrays/maps, manual caching and persistence hints, join strategy controls, advanced grouping, richer compact lineage, and optional field-level lineage.

### v3

Full streaming orchestration: generated stream reads/writes, triggers, checkpoints, watermarks, and stateful streaming policies.
