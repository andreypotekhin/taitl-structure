# Design: v2 Optimization Features

## Purpose

V2 introduces explicit manual optimization controls while preserving Structure's commitment to Spark-plan-visible generated code.

## Design Stance

V2 should grow compiler-visible feature families, not a second PySpark API. Each addition should expose the smallest
Structure-level semantic concept that users need, represent it in IR, and lower it to optimizer-visible PySpark through
the shared target recipes. Do not add wrappers merely because PySpark has a function with that name.

This keeps the complexity budget bounded:

- common operations become typed, traceable, generated, and parity-tested Structure semantics;
- rare or highly backend-specific operations remain explicit hooks;
- unsupported operations fail before rendering or online execution instead of silently becoming UDFs, RDD work, or
  row-wise Python callbacks.

## Spark Higher-Order Functions

Support array/map expressions without Python UDFs.

Examples:

```python
items_clean = arr_transform(order.items, lambda item: lower(trim(item)))
valid_items = arr_filter(order.items, lambda item: item.is_not_null())
```

Generated code should use Spark SQL higher-order functions or PySpark equivalents.

Higher-order callbacks must be symbolic callbacks over Structure expressions. They must not accept arbitrary Python
callbacks that would require row-wise Python execution or hidden UDF generation.

## Caching and Persistence

Allow explicit annotations at subtransform boundaries.

```python
@cache(StorageLevel.MEMORY_AND_DISK)
def add_customer(self, order: OrderNormalized) -> OrderWithCustomer:
    ...
```

Generated code:

```python
df = df.persist(StorageLevel.MEMORY_AND_DISK)
```

Caching must be explicit. Structure should not silently cache.

## Join Strategies

Extend join hints beyond broadcast.

```python
customer = join_one(
    self.customers,
    on=self.customers.id == order.customer_id,
    how=Join.LEFT,
    strategy=JoinStrategy.BROADCAST,
)
```

Potential strategies:

- auto
- broadcast
- shuffle_hash
- sort_merge
- shuffle_replicate_nl

## Advanced Aggregation and Grouping

Support:

- group by
- multi-key group by
- grouping sets
- rollup
- cube
- approximate aggregates
- filtered aggregates if feasible

Example:

```python
def daily_customer_totals(self, order: OrderEnriched) -> CustomerDailyTotal:
    group_by(
        customer_id=order.customer_id,
        order_date=to_date(order.order_ts),
    )

    return CustomerDailyTotal(
        customer_id=order.customer_id,
        order_date=to_date(order.order_ts),
        order_count=count(),
        gross_total=sum(order.total),
    )
```

Aggregation support should model grouping keys, aggregate expressions, filters, output shape, and null/type behavior as
Structure semantics first. The PySpark target may lower those semantics to `groupBy(...).agg(...)`, window expressions,
or compatible target syntax, but the public DSL should stay smaller than Spark's full aggregation API.

## Performance Guardrails

All optimization features should be explicit and visible in generated code. They should never hide Python UDFs or row-wise execution.
