# Full PySpark Join Support

Full PySpark join support covers joins beyond lookup enrichment: right outer joins, full outer joins, explicit cross
joins, non-equi predicates, disjunctive predicates, and join strategy directives. Use it when the output is naturally a
joined rowset rather than one current row enriched with one right-side scope.

The narrower join helpers remain preferred for common cases:

- use `join_one(...)` for zero-or-one lookup enrichment;
- use `join_many(...)` for ordinary row multiplication from a left or inner join;
- use `exists(...)` and `not_exists(...)` for semi and anti filters;
- use temporal and as-of helpers for time-aware select-one lookups.

## Rowset Joins

Broad joins use `join_rowset(...)` or a shortcut helper:

```python
full_join(on=order.customer_id == customer.id)

return OrderCustomerReconciliation.project()(
    order_id=order.id,
    customer_id=customer.id,
    matched=order.id.is_not_null() & customer.id.is_not_null(),
)
```

Available shortcuts are `left_join(...)`, `inner_join(...)`, `right_join(...)`, `full_join(...)`, and
`cross_join(...)`. Predicate shortcuts can be called without assignment when the right relation can be inferred from
`on`.

## Supported Join Types

`Join.INNER` keeps matching row pairs.

`Join.LEFT` keeps every left row and matching right rows.

`Join.RIGHT` keeps every right row and matching left rows.

`Join.FULL` keeps matching rows and unmatched rows from both sides.

`Join.CROSS` emits every left/right pair. It requires `allow_cartesian=True` and does not accept `on`.

## Predicates

`on` accepts compileable symbolic boolean expressions:

- equality and null-safe equality;
- inequalities;
- deterministic expression helpers;
- boolean `AND` and `OR`;
- mixed boolean expressions when every part is compileable.

Structure rejects string SQL fragments, raw column-name strings, Python lambdas, arbitrary Python functions, and
predicates that reference scopes outside the join.

## Output Rules

Right and full joins may produce rows without a left-side current row. Use explicit projection:

```python
return Output.project()(...)
```

Do not use a current-row base constructor after a right or full join:

```python
return Output.base(order)(...)
```

Structure never appends all fields from either side automatically. Choose final output fields explicitly.

## Cross Joins

Cross joins are intentionally noisy:

```python
cross_join(calendar_day, allow_cartesian=True)
```

The acknowledgement prevents an accidentally missing predicate from creating a Cartesian product.

## Example Project

The checked-in orders example includes a compact rowset-join transform:

- source: `examples/orders/transforms/rowset_join.py`;
- generated PySpark: `examples/structure_generated/orders/pyspark/transforms/rowset_join.py`.

It demonstrates a bare `full_join(...)`, a bare `right_join(...)`, and an explicit `cross_join(...,
allow_cartesian=True)`.

## Compatibility

The first implementation treats right, full, cross, non-equi, and disjunctive rowset joins as batch-only. Existing
stream-static compatibility for simpler left and inner joins is unchanged.

Spark Connect support is checked through backend capabilities. Unsupported join forms fail before generated code or
online execution.

## Diagnostics

Diagnostics identify the join type, predicate, nullable sides, cardinality shape, and suggested source fix. Common
fixes include adding `allow_cartesian=True`, using `Output.project()(...)` after full joins, or choosing a narrower
helper such as `join_one(...)`, `join_many(...)`, `exists(...)`, or `not_exists(...)`.
