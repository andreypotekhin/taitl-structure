# Inspecting and maintaining Delta tables in transforms

Inspect recent commits or improve file layout on an existing Delta table without changing its logical rows.
Use typed relation returns for history and detail, and invoke maintenance through explicit effect steps.
The caller configures a Delta-capable Spark session and supplies the native `DeltaTable` handle.

## Inspect history and detail

```python
from structure import Schema, Transform, output, variable
from structure.plugin.pyspark import array, delta_detail, delta_history, delta_input, long, string


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class Commit(Schema):
    version = long()
    operation = string()


class Detail(Schema):
    format = string()
    location = string()
    clustering_columns = array(string(), contains_null=False, alias="clusteringColumns")


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

## Compaction and Z-ordering

Compaction combines small files. Z-ordering groups related values into files so selective queries can skip more
files. Use separate transforms to choose the operation at invocation time. Z-order keys below are non-partition
columns; `order_date` selects partitions for the scoped example.

Run this example with a caller-configured Delta-capable `spark` session on one of the
[admitted runtimes](../compatibility/DeltaIceberg.compat.md). This temporary local table assumes Spark and the client
share a filesystem; remote sessions need a table location accessible to the Spark server. Native Delta calls create
the table, and the example deletes it when finished. Each invocation is independent; normal applications choose
and schedule the maintenance they need.

```python
from tempfile import TemporaryDirectory

from delta.tables import DeltaTable
from structure import Schema, StructureSession, Transform, variable
from structure.plugin.pyspark import delta_optimize, delta_table, string


class Order(Schema):
    customer_id = string(nullable=False)
    product_id = string(nullable=False)
    order_date = string(nullable=False)


class CompactOrders(Transform):
    orders = delta_table(Order)

    def compact(self, order: Order) -> None:
        delta_optimize(order).execute_compaction()

class ZOrderOrders(Transform):
    orders = delta_table(Order)

    def optimize(self, order: Order) -> None:
        delta_optimize(order).execute_zorder(by=(order.customer_id, order.product_id))


class ZOrderPartition(Transform):
    orders = delta_table(Order)
    selected_date = variable(str)

    def optimize(self, order: Order) -> None:
        delta_optimize(order, where=order.order_date == self.selected_date).execute_zorder(
            by=(order.customer_id, order.product_id),
        )


session = StructureSession(spark=spark)
with TemporaryDirectory(prefix="structure-zorder-") as table_path:
    # Table creation, partitioning, and statistics configuration are caller-owned.
    spark.sql(f"""
        CREATE TABLE delta.`{table_path}` (
            customer_id STRING NOT NULL,
            product_id STRING NOT NULL,
            order_date STRING NOT NULL
        ) USING DELTA
        PARTITIONED BY (order_date)
        TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '3')
    """)
    # Two writes produce files in both partitions; this small fixture demonstrates usage.
    spark.sql(f"""INSERT INTO delta.`{table_path}` VALUES
        ('c1', 'p2', '2026-10-07'), ('c2', 'p1', '2026-10-08')""")
    spark.sql(f"""INSERT INTO delta.`{table_path}` VALUES
        ('c2', 'p2', '2026-10-07'), ('c1', 'p1', '2026-10-08')""")
    table = DeltaTable.forPath(spark, table_path)

    # Full-table Z-ordering; this transform does not run compaction as another step.
    result = ZOrderOrders(orders=table).run(session)
    assert result.orders is table

    # Alternatively, select a partition with a runtime value.
    scoped = ZOrderPartition(orders=table, selected_date="2026-10-08").run(session)
    assert scoped.orders is table

    # Invoke CompactOrders(orders=table).run(session) separately when compaction is wanted.
    assert DeltaTable.forPath(spark, table_path).toDF().count() == 4
```

`by=` requires a non-empty tuple of distinct top-level fields from the target table. An optimize `where=` predicate
may reference partition columns only and selects entire partitions, not rows. Native Delta validates layout and key
requirements. Both operations preserve rows and schema; native metric DataFrames are not returned. The transform
retains the original table handle, so reopen the table when a fresh DataFrame snapshot is needed.

Choose keys used in selective filters and ensure their statistics are collected. More keys can dilute the benefit;
Z-ordering rewrites files and does not guarantee query-result ordering. It cannot be applied to liquid-clustered
tables. See [choosing Z-order columns](../background/DeltaIceberg.back.md#choosing-z-order-columns) and the
[operation reference](../reference/DeltaIceberg.ref.md#compaction-and-z-ordering).

## Liquid clustering

The caller configures liquid-clustering keys on the native Delta table. `delta_optimize(order).execute_compaction()`
delegates incremental clustering to those keys. Use `.full()` to recluster existing data. It requires active keys.
On admitted open-source Delta profiles, clustering-enabled tables reject optimize predicates and Z-order, even after
keys are cleared. Databricks `OPTIMIZE FULL WHERE` is outside this admission. See the
[liquid-clustering reference](../reference/DeltaIceberg.ref.md#liquid-clustering).

## Restore and vacuum

Restore and vacuum remain independently invoked maintenance effects. Using the `Order` schema and table appropriate
to your application:

```python
from structure import Transform, variable
from structure.plugin.pyspark import delta_restore, delta_table, delta_vacuum


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

See [Delta Tables API](../api/DeltaIceberg.api.md) for table metadata declarations and
[compatibility](../compatibility/DeltaIceberg.compat.md) for the runtime admission status.
