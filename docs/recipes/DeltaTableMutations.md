# Mutate a Delta Table in a Transform

**Problem:** An existing orders table needs cancelled rows removed and incoming order changes applied.

**Solution:** Bind the native table handle with `delta_table` and declare delete and merge effects in a typed step.

## Define the rows and mutations

The caller supplies a Delta-capable Spark session and an existing table matching `Order`. The incoming DataFrame
matches `Change`:

```python
from structure import *
from structure.plugin.pyspark import *


class Order(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)
    constraints = (check(status != "invalid", name="valid_status"),)


class Change(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)


class ApplyOrderChanges(Transform):
    changes = input(Change)
    orders = delta_table(Order)

    def apply(self, change: Change, order: Order) -> None:
        delta_delete(order, where=order.status == "cancelled")
        (
            delta_merge(order, change, on=order.id == change.id)
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
inserted unless they are cancelled. Supply at most one incoming change per `id` for this example; native Delta
controls multiple-match behavior.

## Provision the expected CHECK

`Schema.constraints` describes a constraint the table must already contain. Create the matching native CHECK before
running the transform, unless the table already has it:

```sql
ALTER TABLE delta.`/path/to/orders`
ADD CONSTRAINT valid_status CHECK (status <> 'invalid')
```

Structure verifies the declared metadata, and Delta enforces the predicate on writes. Structure does not install the
constraint or create the table.

## Run against an existing table

Pass a native `DeltaTable` handle, rather than its DataFrame. Here `orders_path` names the caller's existing table:

```python
from delta.tables import DeltaTable


session = StructureSession(spark=spark)
table = DeltaTable.forPath(spark, orders_path)
result = ApplyOrderChanges(changes=changes_df, orders=table).run(session)
assert result.orders is table
```

The compiler infers the target from the unique `delta_table(Order)` binding. Use `@step` to select relations explicitly
when multiple candidates share a schema. A merge-only step may return `.execute()` and declare `-> Order`; that return
still represents a table mutation and retains the caller's handle.

## Choose a smaller operation

For a status normalization without an incoming relation, use `delta_update`:

```python
class NormalizeOrderStatus(Transform):
    orders = delta_table(Order)

    def normalize(self, order: Order) -> None:
        delta_update(order, where=order.status == "legacy", set=Order(status="pending"))
```

Use `delta_append(order, change).execute()` when every incoming row should be appended without matching existing
rows. Update and delete require an explicit `where=` predicate; `where=True` affects every row. Read-only `delta_input`
bindings cannot be used for these same-schema row mutations.

Each effect commits separately. A failed merge does not undo the earlier deletion, and Structure does not retry
uncertain commits. Reopen the table when inspecting a fresh snapshot after a write.

See the [Delta reference](../reference/DeltaIceberg.ref.md#same-schema-mutations),
[CHECK rules](../reference/DeltaIceberg.ref.md#check-constraints), and
[compatibility ledger](../compatibility/DeltaIceberg.compat.md) for the admitted runtimes.
