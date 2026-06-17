# Design: v2 Optimization Features

## Purpose

V2 introduces explicit manual optimization controls while preserving Structure's commitment to Spark-plan-visible generated code.

## Spark Higher-Order Functions

Support array/map expressions without Python UDFs.

Examples:

```python
items_clean = arr_transform(order.items, lambda item: lower(trim(item)))
valid_items = arr_filter(order.items, lambda item: item.is_not_null())
```

Generated code should use Spark SQL higher-order functions or PySpark equivalents.

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
customer = self.customers.join_one(
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

## Performance Guardrails

All optimization features should be explicit and visible in generated code. They should never hide Python UDFs or row-wise execution.
