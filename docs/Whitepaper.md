# Structure: Typed Schema Transform Compilation for PySpark

## Abstract

Structure is an open-source Python DSL and code generator for building schema-enforced, IDE-friendly, Spark-optimized data pipelines. It lets developers describe data processing as typed schema-to-schema transformations while generating clean PySpark DataFrame code suitable for Airflow, Spark jobs, and batch data platforms.

Structure is designed for teams that want the maintainability of object-style schema transformations without giving up Spark's optimizer-friendly DataFrame execution model.

The central idea is simple: author compact, typed transformation code; generate explicit, reviewable PySpark Column and DataFrame expressions.

## Problem

Large-scale data pipelines are often written directly in PySpark DataFrame code, SQL, or table-oriented transformation frameworks. These approaches are powerful, but they can become difficult to maintain when business logic is naturally expressed as transformations between nested records, domain objects, or stable schemas.

Common pain points include:

- Weak schema enforcement across multi-step pipelines.
- Transformations represented through column-name strings.
- Limited IDE navigation for fields and transformation logic.
- Hard-to-review dynamically assembled DataFrame code.
- Business logic hidden in arbitrary Python functions or UDFs.
- Airflow DAGs overloaded with transformation internals.
- Difficulty separating generated compiler-checked logic from custom PySpark escape hatches.
- Unclear intermediate pipeline states.
- Hidden performance regressions caused by row-wise Python execution.

Structure addresses these problems by providing a typed source DSL that compiles to explicit PySpark code.

## Performance and Optimization Rationale

Structure's focus on generated PySpark is not merely a code-generation preference. It is a performance strategy.

Spark optimizes work that remains visible in its logical plan. Projection, filtering, joins, predicate pushdown, column pruning, aggregation planning, broadcast joins, whole-stage code generation, and many runtime optimizations depend on transformations being expressed through Spark's DataFrame and Column APIs.

If Structure accepted arbitrary Python logic inside compiled transforms, it would have to generate one of the following:

- Python UDFs.
- pandas UDFs.
- row-wise maps.
- RDD operations.
- opaque callback hooks.

Those forms are sometimes useful, but they reduce optimizer visibility and can introduce serialization overhead or runtime surprises. Structure therefore rejects unsupported compiled-transform code and asks developers to either rewrite it using Structure's expression DSL, define an `@expr_fn` helper, or move arbitrary logic into an explicit hook.

This principle can be summarized as:

```text
Make the fast path pleasant.
Make the slow path explicit.
Never silently choose the slow path.
```

## Design Goals

1. **Schema-first transformation design**  
   Pipelines should be described as transformations between typed schemas.

2. **IDE-friendly authoring**  
   Developers should be able to jump to schema declarations, transform classes, helper functions, and hook methods.

3. **Spark-optimized execution**  
   Compiled transformations should lower to PySpark DataFrame and Column expressions, not row-wise Python functions.

4. **Generated code visibility**  
   Generated PySpark code should be deterministic, readable, and suitable for code review.

5. **Explicit escape hatches**  
   Arbitrary PySpark code should be allowed only through explicit hooks, never through silent fallback.

6. **Convention with optional configuration**  
   The common case should work by convention, while a small TOML config should support repeatable builds and project-wide defaults.

7. **Minimal string references**  
   Schema fields, joins, transforms, hooks, and helpers should be referenced as Python symbols wherever possible.

8. **Fast compiler feedback**  
   Compilation should be fast enough to run during local development and CI.

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
        where(order.customer_id.is_not_null())
        where(order.product_id.is_not_null())

        return OrderNormalized(
            id=order.id,
            customer_id=self.clean_id(order.customer_id),
            product_id=self.clean_id(order.product_id),
            total=to_decimal(order.total, precision=12, scale=2),
        )

    def add_customer(self, order: OrderNormalized) -> OrderWithCustomer:
        customer = self.customers.join_one(
            on=self.customers.id == order.customer_id,
            how=Join.LEFT,
            hint=JoinHint.BROADCAST,
        )

        return OrderWithCustomer(
            id=order.id,
            customer_name=customer.name,
            total=order.total,
        )
```

Public instance methods with schema return annotations are compiled as subtransforms. Subtransforms execute in source order. Their return types form the intermediate schema chain.

```text
OrderRaw -> OrderNormalized -> OrderWithCustomer -> OrderEnriched
```

## Generated PySpark Model

Structure generates one class per transform class.

```python
class EnrichOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()  # only when hooks exist

    def run(self, *, orders: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
        ...
```

Generated code uses Spark DataFrame operations such as:

- `where(...)`
- `select(...)`
- `join(...)`
- `alias(...)`
- `cast(...)`
- `functions.lower(...)`
- `functions.trim(...)`
- `functions.broadcast(...)`

If a transform has no hooks, generated code does not import the source transform class at runtime. This keeps hook-free generated code clean and standalone.

## Less Code Without Hiding Runtime Behavior

Structure source code is shorter because it focuses on semantic schema transitions.

The generated code is intentionally more verbose because it makes runtime behavior explicit:

- input validation
- intermediate validation
- filtering
- projection
- joins
- hook calls
- final projection
- final validation

This split gives developers compact authoring and operations teams reviewable PySpark.

## Schema Enforcement

Structure validates schemas at multiple layers:

1. Compile-time schema field existence.
2. Compile-time type compatibility.
3. Runtime input schema validation.
4. Runtime intermediate schema validation by default.
5. Runtime final output schema validation.

Intermediate validation is enabled by default because each subtransform has a typed return schema. It uses schema-only
checks by default, can opt into fuller constraint validation, and can be disabled project-wide, class-wide, or per
subtransform when needed.

## Filtering

Filtering uses `where(...)` inside compiled subtransforms.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    where(order.total.is_not_null())
    return OrderNormalized(...)
```

Multiple `where(...)` calls are combined with logical AND.

## Expression Helpers

Expression helpers are compileable reusable functions.

```python
@expr_fn
def clean_id(value):
    return lower(trim(value))
```

Class-local expression helpers do not take `self`, but may be called through `self` for IDE discoverability.

```python
customer_id=self.clean_id(order.customer_id)
```

Expression helpers are symbolically executed and inlined into generated Spark expressions.

## Hooks

Hooks are explicit escape hatches for arbitrary PySpark code.

```python
@after(normalize)
def remove_negative_totals(self, *, df, spark, ctx):
    return df.where(F.col("total") >= 0)
```

Hook signature:

```python
def hook_name(self, *, df, spark, ctx) -> DataFrame:
    ...
```

Hooks receive the current DataFrame, SparkSession, and optional context. They do not receive every named input by
default. This keeps the hook ABI small and stable.

Hooks that need original named inputs opt in explicitly:

```python
@after(normalize, pass_inputs=True)
def custom_check(self, *, df, inputs, spark, ctx) -> DataFrame:
    orders = inputs.orders
    return df
```

The `inputs` namespace contains the original `run(...)` input DataFrames after input validation. It is intentionally
not passed unless requested.

## Joins

Joins are symbolic and typed.

```python
customer = self.customers.join_one(
    on=self.customers.id == order.customer_id,
    how=Join.LEFT,
    hint=JoinHint.BROADCAST,
)
```

The joined row scope is then used in the returned schema object.

```python
customer_name=customer.name
```

Serial joins are N-step enrichment chains. They are not limited to three inputs.

## Streaming Compatibility

Structure v1 and v2 do not generate streaming orchestration. They generate DataFrame transforms that can operate on streaming DataFrames when the operations used are compatible with Spark Structured Streaming.

The caller owns:

- `readStream`
- `writeStream`
- output mode
- trigger
- checkpoint
- lifecycle

Full streaming orchestration belongs to v3.

## Compatibility Policy

Structure v1 targets Python 3.11+ and generated PySpark for PySpark 3.5.x and 4.0.x. The default project setting is
`target_pyspark = ">=3.5,<4.1"`.

Generated code targets ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs. Spark Connect support is
scheduled for v3 with backend and orchestration work, unless it can be added earlier without changing Structure source
syntax, generated class construction, `run(...)` signatures, or generated-code reviewability.

Generated PySpark, compiler lineage metadata, and configuration each have explicit versioning rules. The public policy
lives in `docs/Compatibility.md`.

## Lineage

Structure records compact compiler lineage by default.

Compiler provenance maps source nodes to IR nodes to generated PySpark nodes. Static dataflow lineage records inferred
transform, table, and column dependencies from the IR. Together, they let diagnostics explain where generated code came
from and which upstream inputs affect a failing field or step.

Hook boundaries are explicit. Because hooks contain arbitrary PySpark, static dataflow should mark them opaque unless a
future compiler-visible hook contract says otherwise.

Runtime LDJSON lineage is useful transform-run telemetry, but it is beyond the v1, v2, and v3 roadmap.

## Unsupported Code Detection

Unsupported code detection is a performance feature as much as a correctness feature.

Unsupported source:

```python
customer_id=order.customer_id.strip().lower()
```

Structure rejects this because Python string methods on symbolic expressions cannot be compiled directly to Spark Column expressions.

A structured error should include:

- transform class
- subtransform method
- output field
- source expression
- problem
- performance rationale
- direct DSL alternative
- `@expr_fn` helper alternative
- hook alternative
- configuration workaround when one exists

Example guidance:

```text
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

Configuration workaround:
  No configuration setting allows unsupported Python string methods inside compiled transforms.
  This is intentional because compiled transforms must remain Spark-plan-visible.
```

For validation-related errors, a configuration workaround may exist:

```text
Configuration workaround:
  Set validate_intermediate = false to skip intermediate runtime schema validation.
  Set intermediate_validation_mode = "schema_only" to avoid row-level intermediate checks.
  This does not change compile-time field/type checking.
```

## Compiler Performance

Structure should be fast enough to run during normal development and CI.

Compile-time performance is a product feature. Implementation should track metrics such as:

- number of discovered modules
- number of transform classes
- symbolic execution time
- IR check time
- code generation time
- formatting time
- compiler provenance time
- static dataflow lineage time
- total wall-clock time

The compiler should avoid starting Spark during normal compile/check operations.

Recommended implementation techniques:

- incremental compilation based on source fingerprints
- compiler cache directory
- parallel code generation
- lazy module inspection where possible
- fast IR tests that do not require Spark
- optional formatting only when generated content changes

## Roadmap

### v1

Projection, filtering, joins, typed intermediate schemas, generated PySpark classes, hooks, validation, compiler
provenance, static dataflow lineage, and streaming-compatible transforms.

### v2

Aggregations, windowing, advanced grouping, Spark higher-order functions, caching/persistence hints, join strategy
annotations, and richer static dataflow explain output.

### v3

Full streaming orchestration: `readStream`, `writeStream`, triggers, checkpoints, watermarks, advanced stateful
streaming policies, and Spark Connect support.

## Summary

Structure provides a middle path between hand-written PySpark and purely table-oriented transformation frameworks.

It gives developers a schema-oriented authoring model while producing optimized, explicit, reviewable PySpark code.
