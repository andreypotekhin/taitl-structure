# Structure: Typed Schema Transform Compilation for PySpark

## Abstract

Structure is an open-source Python DSL and runtime/compiler toolkit for building schema-enforced, IDE-friendly,
Spark-optimized data pipelines. It lets developers describe data processing as typed schema-to-schema transformations
while running or generating clean PySpark DataFrame code suitable for Airflow, Spark jobs, and batch data plugins.

Structure is designed for teams that want object-oriented data transformations without giving up Spark optimizer-friendly DataFrame execution model.

The idea is simple: write compact, typed data transformation code, execute as optimizer-visible PySpark, optionally generate explicit PySpark artifacts.

## Problem

Large-scale data pipelines are usually written in PySpark DataFrame code, SQL, or table-oriented transformation frameworks. They can become difficult to maintain when business logic becomes non-trivial.

Common pain points include:

- Weak schema enforcement across multi-step pipelines.
- Orchestration DAGs overloaded with transformation details.
- Transformations represented through column-name strings/aliases.
- Limited IDE navigation caused by heavy use of string literals.
- Hard-to-review organically evolving DataFrame code.
- Weak enforcement of intermediate schemas between pipeline stages.
- Mixing of optimizer-friendly code with opaque escape hatches such as UDFs.
- Hidden performance regressions caused by row-wise Python executions.

Structure attempts to address these problems by providing a typed DSL that compiles to  
PySpark operations. This allows code author to deal with classes, fields and methods instead 
of dealing with strings and freely-mutating data frames. 

## Performance and Optimization Rationale

Structure's focus on PySpark DataFrame and Column operations is not merely an implementation preference - it is a
performance strategy. Spark optimizes work that remains visible in its logical plan. Projection, filtering, joins, predicate pushdown, column pruning, aggregation planning, broadcast joins, whole-stage code generation, and many runtime optimizations depend on transformations being expressed through Spark's DataFrame and Column APIs.

If Structure accepted arbitrary Python logic inside compiled transforms, it would have to generate one of the following:

- Python UDFs.
- pandas UDFs.
- row-wise maps.
- RDD operations.
- opaque callback hooks.

Those forms are sometimes useful, but they reduce optimizer visibility and can introduce serialization overhead or runtime surprises. Structure therefore rejects unsupported compiled-transform code and asks developers to either rewrite it using Structure's expression DSL or move arbitrary logic into an explicit hook. This principle can be summarized as:

```text
Make the fast path pleasant.
Make the slow path explicit.
Never silently choose the slow path.
```

## Design Goals

1. **Schema-first data transformations**
   Pipelines should be described as transformations between typed schemas.

2. **IDE-friendly authoring**
   Developers should be able to jump to schema declarations, transform classes, stages, steps, API function definitions.

3. **Spark-optimized execution**
   Compiled transformations should lower to PySpark DataFrame and Column expressions, not row-wise Python functions.

4. **Runtime and generated-code visibility**
   Execution should preserve code semantics, and optional code generation should be deterministic,
   readable, and suitable for code review.

5. **Explicit escape hatches**
   Arbitrary PySpark code should be allowed only through explicit hooks, never through silent fallback.

6. **Convention with optional configuration**
   The common case should work by convention, while a small TOML config should support repeatable builds and project-wide defaults.

7. **Minimal string references**
   Schema fields, joins, transforms, hooks, and helpers should be referenced as Python symbols wherever possible.

8. **Fast compiler feedback**
   Compilation should be fast enough to run during local development and CI.

## Core Model

A Structure source transform is a Python class inheriting `Transform`.

```python
class EnrichOrders(Transform):

    orders = input(OrderRaw)
    customers = input(Customer)
    products = input(Product)
    published = output(OrderPublished)

    @special(type="expr")
    def clean_id(value):
        return lower(trim(value))

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        where(order.customer_id.is_not_null())
        where(order.product_id.is_not_null())

        return OrderNormalized.project(order)(
            id=order.id,
            customer_id=self.clean_id(order.customer_id),
            product_id=self.clean_id(order.product_id),
            total=to_decimal(order.total, precision=12, scale=2),
        )

    def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
        left_join(
            on=order.customer_id == customer.id,
            hint="broadcast",
        )
        return OrderWithCustomer.base(order)(
            customer_name=customer.name,
        )
```

Public instance methods with schema return annotations are compiled as step methods. Step methods execute in source order. Their return types form the intermediate schema chain.

```text
OrderRaw -> OrderNormalized -> OrderWithCustomer -> OrderEnriched
```

## Execution and Code Generation Model

Execution is the default:

```python
session = StructureSession(spark=spark, ctx=ctx)

result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
).run(session)

enriched = result.enriched
```

The transform instance is a deferred invocation. `StructureSession` owns Spark, optional context, resolved
configuration, execution mode, target backend, and runner selection.

Structure can also generate one class per transform class.

```python
class EnrichOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()  # only when hooks exist

    def run(self, *, orders: DataFrame, customers: DataFrame, products: DataFrame) -> TransformResult:
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

If a transform has no hooks, generated code does not import the source transform class at runtime. This keeps hook-free
generated code clean and standalone in generated mode.

## Less Code Without Hiding Runtime Behavior

Structure source code is shorter because it focuses on semantic schema transitions.

Optional generated code is intentionally more verbose because it makes runtime behavior explicit:

- input validation
- intermediate validation
- filtering
- projection
- joins
- hook calls
- final projection
- final validation

This split gives developers compact authoring, execution by default, and reviewable PySpark when teams want it.

## Schema Enforcement

Structure validates schemas at multiple layers:

1. Compile-time schema field existence.
2. Compile-time type compatibility.
3. Runtime input schema validation.
4. Runtime intermediate schema validation by default.
5. Runtime final output schema validation.

Intermediate validation is enabled by default because each step method has a typed return schema. It uses schema-only
checks by default, can opt into fuller constraint validation, and can be disabled project-wide, class-wide, or per
step method when needed.

## Filtering

Filtering uses `where(...)` inside compiled step methods.

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
@special(type="expr")
def clean_id(value):
    return lower(trim(value))
```

Class-local expression helpers do not take `self`, but may be called through `self` for IDE discoverability.

```python
customer_id=self.clean_id(order.customer_id)
```

Expression helpers are symbolically executed and lowered into execution recipes or generated Spark expressions.

## Hooks

Hooks are explicit escape hatches for arbitrary PySpark code.

```python
@raw(inout=lane(orders) | lane(orders))
def remove_negative_totals(self, *, orders, spark, ctx):
    return orders.where(F.col("total") >= 0)
```

Hook signature:

```python
def hook_name(self, *, selected_lane_name, spark, ctx) -> DataFrame:
    ...
```

Hooks receive the DataFrames selected by their binding, SparkSession, and optional context. The binding keeps the hook
ABI small and explicit.

Hooks can explicitly select both the current lane and an original input:

```python
@raw(inout=[lane(orders), input(customers)] | lane(orders))
def custom_check(self, *, orders, customers, spark, ctx) -> DataFrame:
    customer_ids = customers.select("id")
    return orders
```

`input(customers)` selects the original runtime input; `lane(orders)` selects the current intermediate lane. Every
selected DataFrame is passed by its declared name.

## Joins

Joins are symbolic and typed.

```python
left_join(
    on=order.customer_id == customer.id,
    hint="broadcast",
)
```

The joined row scope is then used in the returned schema object.

```python
customer_name=customer.name
```

Serial joins are N-step enrichment chains. They are not limited to three inputs.

## Streaming Compatibility

Structure does not generate streaming lifecycle code. It generates DataFrame transforms that can operate on streaming
DataFrames when the operations used are compatible with Spark Structured Streaming.

The caller owns:

- `readStream`
- `writeStream`
- output mode
- trigger
- checkpoint
- lifecycle

Lifecycle remains caller-owned permanently. The current streaming ledger expands transformation coverage, state
diagnostics, and live streaming evidence. It covers adoption APIs, lifecycle boundaries, output-mode
requirements, watermarks, event-time/session windows, bounded dedupe, admitted stream-static and bounded stream-stream
joins, and explicit deferred/unsupported stateful families.

## Compatibility Policy

Structure targets Python 3.11+ and execution/generated-code execution for PySpark 3.5.x and 4.0.x. The default project
settings are `execution_mode = "online"` and the PySpark plugin options `profile = ">=3.5,<4.1"` and
`variant = "ordinary"` under `[tool.structure.plugin.pyspark]`.

Execution and generated-code execution target ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs by default.
Spark Connect supports completed compiler-visible batch features; streaming remains caller-owned ordinary PySpark work.
It does not change Structure source syntax, execution invocation construction, generated class construction, `run(...)`
signatures, or generated-code reviewability.

Generated PySpark, compiler traceability metadata, and configuration each have explicit versioning rules. The public policy
lives in [Compatibility.md](Compatibility.md).

## Traceability

Structure records compact compiler traceability by default.

Compiler provenance maps source nodes to IR nodes to generated PySpark nodes. Static dataflow traceability records inferred
transform, table, and column dependencies from the IR. Together, they let diagnostics explain where generated code came
from and which upstream inputs affect a failing field or step.

Hook boundaries are explicit. Because hooks contain arbitrary PySpark, static dataflow should mark them opaque unless a
future compiler-visible hook contract says otherwise.

Runtime LDJSON traceability is useful transform-run telemetry, but it is beyond the published roadmap.

## Unsupported Code Detection

Unsupported code detection is a performance feature as much as a correctness feature.

Unsupported source:

```python
customer_id=order.customer_id.strip().lower()
```

Structure rejects this because Python string methods on symbolic expressions cannot be compiled directly to Spark Column expressions.

A structured error should include:

- transform class
- step method
- output field
- source expression
- problem
- performance rationale
- direct DSL alternative
- `@special(type="expr")` helper alternative
- hook alternative
- configuration workaround when one exists

Example guidance:

```text
Use direct DSL functions:
  customer_id=lower(trim(order.customer_id))

For reuse:
  @special(type="expr")
  def clean_id(value):
      return lower(trim(value))

For arbitrary PySpark:
  @raw(inout=lane(orders) | lane(orders))
  def clean_id_column(self, *, orders, spark, ctx):
      return orders.withColumn("customer_id", F.lower(F.trim(F.col("customer_id"))))

Configuration workaround:
  No configuration setting allows unsupported Python string methods inside compiled transforms.
  This is intentional because compiled transforms must remain Spark-plan-visible.
```

For validation-related errors, a configuration workaround may exist:

```text
Configuration workaround:
  Set validate_intermediate = false to skip intermediate runtime schema validation.
  Set input_validation_mode, intermediate_validation_mode, or output_validation_mode to "schema_only"
  to avoid row-level checks at that phase.
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
- static dataflow traceability time
- total wall-clock time

The compiler should avoid starting Spark during normal compile/check operations.

Recommended implementation techniques:

- source fingerprints that enable future production incremental compilation
- compiler cache directory
- parallel code generation
- lazy module inspection where possible
- fast IR tests that do not require Spark
- optional formatting only when generated content changes

## Roadmap

The roadmap follows an IR-first north star: the initial release proves that Structure can replace hand-maintained
PySpark boilerplate with strict execution and optional code-generation workflow. It now supports mainstream analytical
pipelines, completed Spark Connect batch features, compiler-visible streaming transformations, and broad typed PySpark
coverage while loading, storage, and orchestration remain caller-owned.

### Initial Release

PySpark execution by default, optional generated PySpark classes, projection, filtering, joins, typed
intermediate schemas, hooks, validation, compiler provenance, compact static dataflow traceability, streaming-compatible
transforms, diagnostic links, and setup checks.

### Current Capability Boundary

Structure provides typed, compiler-visible PySpark transformations across expressions, nested values, relation
operations, joins, aggregations, windows, collections, and the admitted streaming shapes. The checked coverage catalog
separates a Structure equivalent from a deliberate boundary, with a companion streaming ledger for adoption APIs and
stateful/lifecycle boundaries. Source, sink, checkpoint, trigger, output-mode application, query lifecycle, loading,
storage, orchestration, and actions remain caller-owned.

## Summary

Structure provides a middle path between hand-written PySpark and purely table-oriented transformation frameworks.

It gives developers a schema-oriented authoring model while producing optimized, explicit, reviewable PySpark code.
