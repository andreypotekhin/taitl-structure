# Mutate a Delta Table in a Transform

This recipe binds an existing table for same-schema updates. The caller creates the table and installs its native
constraints before running the transform.

```python
from structure import Schema, StructureSession, Transform, input
from structure.plugin.pyspark import check, delta_delete, delta_merge, delta_table, integer, string


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
        (delta_merge(order, change, on=order.id == change.id)
         .when_matched_update_all()
         .when_not_matched_insert_all()
         .execute())


result = ApplyOrderChanges(changes=changes_df, orders=delta_table_handle).run(
    StructureSession(spark=spark)
)
assert result.orders is delta_table_handle
```

The compiler infers the same-schema Delta effect from the typed parameters and unique `delta_table(Order)` binding. If
the step has multiple candidate relations, use `@step` to select them explicitly. For merge-only transforms, the method
may instead return `.execute()` and declare `-> Order`; the compiler verifies the return schema against the target.

To declare expected native constraint metadata, add `constraints = (check(...),)` to the Structure schema and install
the matching Delta CHECK on the table:

```sql
ALTER TABLE delta.`/path/to/orders`
ADD CONSTRAINT valid_status CHECK (status <> 'invalid')
```
