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

## Inputs

Inputs are named class attributes.

```python
orders = input(OrderRaw)
customers = input(Customer)
products = input(Product)
```

Generated `run(...)` methods use the same names:

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

Subtransforms run in source order.

```text
OrderRaw -> OrderNormalized -> OrderWithCustomer -> OrderEnriched
```

## Intermediate Validation

Structure validates intermediate schemas by default.

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

Use `where(...)`.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    where(order.total.is_not_null())

    return OrderNormalized(
        id=order.id,
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

Multiple `where(...)` calls are combined with logical AND.

## Add Columns

Add columns by returning a schema with more fields.

```python
class OrderWithFlags(Schema):
    id = field(string)
    total = field(decimal(12, 2))
    is_large = field(boolean)


def add_flags(self, order: OrderRaw) -> OrderWithFlags:
    total = to_decimal(order.total, precision=12, scale=2)

    return OrderWithFlags(
        id=order.id,
        total=total,
        is_large=total > 1000,
    )
```

## Drop Columns

Drop columns by returning a schema with fewer fields.

```python
def public_order(self, order: OrderRaw) -> OrderPublic:
    return OrderPublic(
        id=order.id,
        customer_id=order.customer_id,
        total=to_decimal(order.total, precision=12, scale=2),
    )
```

Generated code uses projection rather than `drop(...)`, producing deterministic output schemas.

## Expression Helpers

Use `@expr_fn` for reusable compileable expressions.

```python
@expr_fn
def clean_id(value):
    return lower(trim(value))
```

Class-local helpers do not take `self`, but may be called through `self`.

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized(
        customer_id=self.clean_id(order.customer_id),
    )
```

## Joins

Declare named inputs:

```python
orders = input(OrderRaw)
customers = input(Customer)
```

Use symbolic joins:

```python
def add_customer(self, order: OrderNormalized) -> OrderWithCustomer:
    customer = self.customers.join_one(
        on=self.customers.id == order.customer_id,
        how=Join.LEFT,
        hint=JoinHint.BROADCAST,
    )

    return OrderWithCustomer(
        id=order.id,
        customer_id=order.customer_id,
        customer_name=customer.name,
        total=order.total,
    )
```

Serial joins are not limited to any fixed number of inputs. Add as many input declarations and source-ordered subtransforms as needed.

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

Hooks receive:

- `self`: the source transform instance.
- `df`: the current DataFrame.
- `spark`: the SparkSession.
- `ctx`: optional pipeline context.

Named input DataFrames are not passed to hooks by default. Hooks that need external data can use `spark`, `ctx`, or explicit reads.

## Manual Optimization Hooks and Hints

Structure's compiled path prioritizes generated Spark expressions. For manual optimization, prefer explicit APIs that remain plan-visible:

- `JoinHint.BROADCAST` and future join strategy hints.
- v2 cache/persist hints at step boundaries.
- v2 higher-order function helpers for arrays and maps.
- v2 advanced aggregation and grouping APIs.
- Explicit hooks for hand-written PySpark when needed.

Structure should not silently introduce performance-compromising fallbacks.

## Unsupported Code

Compiled subtransforms are strict.

Unsupported:

```python
customer_id=order.customer_id.strip().lower()
```

Supported:

```python
customer_id=lower(trim(order.customer_id))
```

Reusable supported helper:

```python
@expr_fn
def clean_id(value):
    return lower(trim(value))
```

Explicit PySpark hook:

```python
@after(normalize)
def clean_id_column(self, *, df, spark, ctx):
    return df.withColumn("customer_id", F.lower(F.trim(F.col("customer_id"))))
```

If a safe configuration escape hatch exists, error details should show it. Example:

```text
Config workaround:
  Set [tool.structure] validate_intermediate = false
  only if this subtransform intentionally produces a temporary schema mismatch.
```

Structure rejects unsupported compiled-transform code to preserve Spark optimizer visibility.
