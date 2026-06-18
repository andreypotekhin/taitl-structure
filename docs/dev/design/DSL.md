# Design: DSL

## Purpose

The DSL is the user-facing API for schemas, transforms, expressions, joins, filters, hooks, and validation policy.

The DSL must be pleasant to write, IDE-friendly, and strict enough to compile into Spark-plan-visible expressions.

## Public API Surface

```python
from structure import (
    Schema,
    field,
    String,
    Integer,
    Long,
    Float,
    Double,
    Decimal,
    Boolean,
    Date,
    Timestamp,
    Array,
    Struct,
    Map,
    Transform,
    transform,
    input,
    expr_fn,
    where,
    before,
    after,
    validate_output,
    lower,
    trim,
    to_decimal,
    when,
    coalesce,
    Join,
    JoinHint,
    SchemaMode,
)
```

## Source Example

```python
@transform
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)

    @expr_fn
    def clean_id(value):
        return lower(trim(value))

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        return OrderNormalized(
            id=order.id,
            customer_id=self.clean_id(order.customer_id),
            total=to_decimal(order.total, precision=12, scale=2),
        )

    @after(normalize)
    def remove_negative_totals(self, *, df, spark, ctx):
        return df.where(F.col("total") >= 0)
```

## Rules

- `@transform` marks classes for generation.
- `input(Schema)` declares named DataFrame inputs.
- Public instance methods returning `Schema` types are compiled subtransforms.
- `where(...)` records filter expressions in the current symbolic context.
- `@expr_fn` functions execute symbolically and must return expressions.
- `@after(method)` and `@before(method)` attach arbitrary PySpark hooks.
- Hooks use signature `def hook(self, *, df, spark, ctx)`.

## Data Flow

```text
User source code
  ↓ import/discovery
DSL metadata objects
  ↓ symbolic execution
Expression and plan IR
  ↓ PySpark emitter
Generated DataFrame code
```

## Compile-Time Performance

The DSL should avoid expensive work during module import. Decorators should attach metadata only. No Spark session should be created. Expression functions should be ordinary Python callables evaluated only during symbolic execution.
