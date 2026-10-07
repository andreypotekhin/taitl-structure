# Inspecting and maintaining Delta tables in transforms

Use typed relation returns to inspect Delta history and detail, and explicit effect calls for table maintenance. The
caller creates the table, enables any required features, and passes the native `DeltaTable` handle.

```python
from structure import Schema, Transform, output, variable
from structure.plugin.pyspark import delta_detail, delta_history, delta_input, long, string


class Commit(Schema):
    version = long()
    operation = string()


class Detail(Schema):
    format = string()
    location = string()


class InspectOrders(Transform):
    orders = delta_input(Order)
    limit = variable(int, default=10)
    commits = output(Commit)
    details = output(Detail)

    def history(self, order: Order) -> Commit:
        return delta_history(order, limit=self.limit)

    def detail(self, order: Order) -> Detail:
        return delta_detail(order)
```

History is newest first. The typed output may expose a subset of Delta's metadata columns. The limit can vary at runtime
without recompiling; it must be positive. `delta_detail` returns one metadata row.

Maintenance belongs in its own step so the effect is visible and cannot be confused with a DataFrame result:

```python
from structure import Transform
from structure.plugin.pyspark import delta_optimize, delta_restore, delta_table, delta_vacuum


class CompactOrders(Transform):
    orders = delta_table(Order)

    def compact(self, order: Order) -> None:
        delta_optimize(order).execute_compaction()

    def sort_files(self, order: Order) -> None:
        delta_optimize(order).execute_zorder(by=(order.id,))


class RestoreOrders(Transform):
    orders = delta_table(Order)
    version = variable(int)

    def restore(self, order: Order) -> Order:
        return delta_restore(order, version=self.version).execute()


class VacuumOrders(Transform):
    orders = delta_table(Order)

    def vacuum(self, order: Order) -> None:
        delta_vacuum(order).execute()
```

Restore creates a new commit. Optimize rewrites files while preserving logical rows. Their native metric DataFrames are
not exposed as transform outputs. An optimize predicate may use only table partition columns. Vacuum defaults to 168
hours; a shorter retention requires `allow_short_retention=True`, and Delta's native retention safety check remains
active. Vacuum removes files that older snapshots may need, so ensure no reader or stream still depends on them.

See [Delta Tables API](../api/DeltaIceberg.api.md) for table metadata declarations and [compatibility](../compatibility/DeltaIceberg.compat.md)
for the runtime admission status.
