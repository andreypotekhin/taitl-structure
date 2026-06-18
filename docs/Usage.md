# Usage

## Transform Classes

A transform class is declared with `@transform`.

```python
@transform
class NormalizeOrders(Transform):
    orders = input(OrderRaw)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        ...
```

Structure generates one PySpark class per transform class.

```python
class NormalizeOrdersGenerated:

    def __init__(self, *, spark, ctx=None):
        self.spark = spark
        self.ctx = ctx

    def run(self, *, orders):
        ...
```

## Source and Generated Paths

Default filesystem layout:

```text
src/pipeline_src/...
generated/structure_generated/pipeline_src/...
```

These paths are configurable. Mark `src` and `generated` as source roots in the IDE.

## Inputs

Inputs are named class attributes.

```python
orders = input(OrderRaw)
customers = input(Customer)
products = input(Product)
```

Generated `run(...)` methods use the same names.

```python
def run(self, *, orders, customers, products):
    ...
```

## Subtransforms

Public instance methods with schema return annotations are compiled as subtransforms.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

Subtransforms execute in source order.

```text
OrderRaw -> OrderNormalized -> OrderWithCustomer -> OrderEnriched
```

## Generated PySpark

A source subtransform like this:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())

    return OrderNormalized(
        id=order.id,
        customer_id=lower(trim(order.customer_id)),
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

generates PySpark like this:

```python
df = orders.where(
    F.col("id").isNotNull()
).select(
    F.col("id").alias("id"),
    F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
    F.col("total").cast("decimal(12,2)").alias("total"),
)
```

## Intermediate Validation

Structure validates intermediate schemas by default.

Project-wide defaults:

```toml
validate_intermediate = true
intermediate_validation_mode = "schema_only"
```

Disable intermediate schema validation project-wide:

```toml
validate_intermediate = false
```

Choose fuller validation only when the added Spark work is intentional:

```toml
intermediate_validation_mode = "schema_and_constraints"
```

```python
@transform(validate_intermediate=True)
class EnrichOrders(Transform):
    ...
```

Disable class-wide:

```python
@transform(validate_intermediate=False)
class EnrichOrders(Transform):
    ...
```

Disable for one method:

```python
@validate_output(False)
def normalize(self, order: OrderRaw) -> OrderNormalized:
    ...
```

## Filtering

Use `where(...)` inside subtransforms.

```python
def valid_orders(self, order: OrderRaw) -> OrderValid:
    where(order.id.is_not_null())
    where(order.total.is_not_null())

    return OrderValid(
        id=order.id,
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

Multiple `where(...)` calls are combined with logical AND.

## Add and Drop Columns

Add columns by returning a schema with more fields.

```python
class OrderWithFlags(Schema):
    id = field(String())
    total = field(Decimal(12, 2))
    is_large = field(Boolean())


def add_flags(self, order: OrderRaw) -> OrderWithFlags:
    total = to_decimal(order.total, precision=12, scale=2)
    return OrderWithFlags(
        id=order.id,
        total=total,
        is_large=total > 1000,
    )
```

Drop columns by returning a schema with fewer fields.

Generated code prefers explicit projection over `drop(...)` so the output schema is deterministic.

## Expression Helpers

Use `@expr_fn` for reusable compileable expressions.

```python
@expr_fn
def clean_id(value):
    return lower(trim(value))
```

Class-local helpers do not take `self`, but can be called through `self`.

```python
customer_id=self.clean_id(order.customer_id)
```

## Joins

Use symbolic joins.

```python
def add_customer(self, order: OrderNormalized) -> OrderWithCustomer:
    customer = self.customers.join_one(
        on=self.customers.id == order.customer_id,
        how=Join.LEFT,
        hint=JoinHint.BROADCAST,
    )

    return OrderWithCustomer(
        id=order.id,
        customer_name=customer.name,
    )
```

Generated PySpark:

```python
df = df.alias("order_normalized")
customers_df = F.broadcast(customers.alias("customers"))

df = df.join(
    customers_df,
    F.col("customers.id") == F.col("order_normalized.customer_id"),
    "left",
).select(
    F.col("order_normalized.id").alias("id"),
    F.col("customers.name").alias("customer_name"),
)
```

Serial joins are N-step enrichment chains and are not limited to any fixed number of inputs.

## Hooks

Hooks are explicit PySpark escape hatches.

```python
@after(normalize)
def remove_negative_totals(self, *, df, spark, ctx):
    return df.where(F.col("total") >= 0)
```

Hook signature:

```python
def hook_name(self, *, df, spark, ctx):
    ...
```

Hooks receive `self`, `df`, `spark`, and `ctx`. Named input DataFrames are not passed to hooks by default.

When a hook needs the original named inputs, opt in explicitly:

```python
@after(normalize, pass_inputs=True)
def check_against_raw_orders(self, *, df, inputs, spark, ctx):
    raw = inputs.orders
    return df
```

`inputs` is a read-only namespace matching the transform's declared input names. It contains the original `run(...)`
input DataFrames, not the current intermediate `df`.

## Streaming Compatibility

Generated transforms operate on DataFrames. If the input DataFrame is streaming and all generated operations are supported by Spark Structured Streaming, the generated transform can be used in a streaming pipeline.

Structure v1/v2 do not generate `readStream` or `writeStream`; the caller owns streaming orchestration.

## Compatibility

v1 generated code targets ordinary PySpark `SparkSession`, `DataFrame`, and `Column` APIs for PySpark 3.5.x and 4.0.x
by default:

```toml
target_pyspark = ">=3.5,<4.1"
```

Spark Connect support is planned for v3 unless it can be added earlier without changing the public DSL or generated
class API. See `docs/Compatibility.md`.

## v2 Manual Optimization Features

Planned v2 features include:

- Spark higher-order functions for arrays and maps.
- Caching and persistence annotations.
- Join strategy annotations.
- Advanced aggregation and grouping.
- Window functions and deduplication helpers.

These features remain explicit because Structure should not hide performance-sensitive choices.
