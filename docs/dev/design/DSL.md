# Design: DSL

## Purpose

The DSL is the user-facing API for schemas, transforms, expressions, joins, filters, hooks, and validation policy.

The DSL must be pleasant to write, IDE-friendly, and strict enough to compile into Spark-plan-visible expressions.

## Public API Surface

```python
from structure import (
    Structure,
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
    output,
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
    StructureSession,
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
    enriched = output(OrderWithCustomer)

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

    def add_customer(self, order: OrderNormalized) -> OrderWithCustomer:
        customer = self.customers.join_one(
            on=self.customers.id == order.customer_id,
            how=Join.LEFT,
            hint=JoinHint.BROADCAST,
        )

        return OrderWithCustomer.base(order)(
            customer_name=customer.name,
            customer_tier=customer.tier,
        )

    @after(normalize)
    def remove_negative_totals(self, *, df, spark, ctx):
        return df.where(F.col("total") >= 0)

    @after(normalize, pass_inputs=True)
    def compare_to_raw(self, *, df, inputs, spark, ctx):
        return df
```

## Rules

- `@transform` marks classes for Structure compilation and execution.
- `Transform.__init__(**inputs)` creates a deferred online invocation by binding declared input DataFrames.
- `Transform.run(session)` delegates execution to `StructureSession`.
- `input(Structure)` declares named DataFrame inputs.
- `output(Structure)` declares one or more named transform results.
- Public instance methods returning `Structure` types are compiled subtransforms.
- `SchemaClass.base(row)(...)` constructs an output schema by copying inherited fields from symbolic base rows and
  overlaying explicit field expressions.
- For multiple direct schema bases, `SchemaClass.base(...)` receives one row per direct base in declaration order.
- `where(...)` records filter expressions in the current symbolic context.
- `@expr_fn` functions execute symbolically and must return expressions.
- `@after(method)` and `@before(method)` attach arbitrary PySpark hooks.
- Hooks use signature `def hook(self, *, df, spark, ctx)`.
- Hooks may opt into original named inputs with `pass_inputs=True` and signature
  `def hook(self, *, df, inputs, spark, ctx)`.

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
