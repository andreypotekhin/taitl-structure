# Mutate an Iceberg Table in a Transform

**Problem:** An existing orders table needs cancelled rows removed and incoming order changes applied.

**Solution:** Bind the catalog identifier with `iceberg_table` and declare delete and merge effects in a typed step.

## Define the rows and mutations

The caller supplies an Iceberg-capable Spark session and an existing format-v2 table whose columns match `Order`.
The incoming DataFrame matches `Change`:

```python
from structure import *
from structure.plugin.pyspark import *


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class Change(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class ApplyOrderChanges(Transform):
    changes = input(Change)
    orders = iceberg_table(Order)

    def apply(self, change: Change, order: Order) -> None:
        iceberg_delete(order, where=order.status == "cancelled")
        (
            iceberg_merge(order, change, on=order.id == change.id)
            .when_matched_delete(condition=change.status == "cancelled")
            .when_matched_update(set=Order(status=change.status))
            .when_not_matched_insert(
                values=Order(id=change.id, status=change.status),
                condition=change.status != "cancelled",
            )
            .execute()
        )
```

Matched cancellations delete the target row. Other matched rows receive their incoming status, and new rows are
inserted unless they are cancelled. Supply at most one incoming change per `id` for this example; native Iceberg and
Spark determine multiple-match behavior.

## Run against an existing table

Pass the fully qualified catalog name at invocation. The result retains that identifier:

```python
session = StructureSession(spark=spark)
result = ApplyOrderChanges(
    changes=changes_df,
    orders="warehouse.sales.orders",
).run(session)
assert result.orders == "warehouse.sales.orders"
```

The compiler infers the effect target from the unique `iceberg_table(Order)` declaration. Use `@step` to select
relations explicitly when multiple candidates share a schema. Read-only `iceberg_input` bindings cannot be used as
same-schema mutation targets.

## Choose a smaller operation

For a status normalization without an incoming relation, use `iceberg_update`:

```python
class NormalizeOrderStatus(Transform):
    orders = iceberg_table(Order)

    def normalize(self, order: Order) -> None:
        iceberg_update(order, where=order.status == "legacy", set=Order(status="pending"))
```

Use `iceberg_append(order, change).execute()` when every incoming row should be appended without matching existing
rows. Update and delete require an explicit `where=` predicate; `where=True` affects every row.

Each effect commits separately. A failed merge does not undo the earlier deletion, and Structure does not retry
uncertain commits. The caller creates the table, manages its catalog, and chooses when to invoke the transform.

See the [Iceberg reference](../reference/DeltaIceberg.ref.md#apache-iceberg-tables) for operation signatures and the
[compatibility ledger](../compatibility/DeltaIceberg.compat.md#apache-iceberg) for admitted runtime profiles.
