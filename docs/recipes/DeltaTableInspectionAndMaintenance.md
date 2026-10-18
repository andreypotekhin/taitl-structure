# Inspect and Maintain a Delta Table in Transforms

**Problem:** An existing orders table needs its commit history inspected, files reorganized, or retained data cleaned
up without hiding those operations inside ordinary row transformations.

**Solution:** Return typed metadata relations and invoke restore, optimization, and vacuum as separate effect
transforms.

The caller configures a Delta-capable Spark session and supplies an existing native `DeltaTable` handle matching
`Order`. The file-layout example below creates a separate disposable table with its own schema.

## Define the table and metadata schemas

```python
from structure import *
from structure.plugin.pyspark import *


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
```

## Inspect history and detail

Bind both metadata methods to the original table input rather than chaining the detail read after the history result:

```python
class InspectOrders(Transform):
    orders = delta_input(Order)
    limit = variable(int, default=10)
    commits = output(Commit)
    details = output(Detail)

    @step(input=orders, output=commits)
    def history(self, order: Order) -> Commit:
        return delta_history(order, limit=self.limit)

    @step(input=orders, output=details)
    def detail(self, order: Order) -> Detail:
        return delta_detail(order)


session = StructureSession(spark=spark)
table = delta_table_handle
inspection = InspectOrders(orders=table, limit=5).run(session)
recent_commits = inspection.commits
table_details = inspection.details
```

History is newest first. The typed output may expose a subset of Delta's metadata columns. The limit can vary at runtime
without recompiling; it must be positive. `delta_detail` returns one metadata row.

## Read a retained version

Return snapshot contents as an ordinary DataFrame when the consumer needs rows from a retained historical version:

```python
class ReadOrderSnapshot(Transform):
    orders = delta_input(Order)
    version = variable(int)
    snapshot = output(Order)

    def read(self, order: Order) -> Order:
        return delta_snapshot(order, version=self.version)


prior_orders = ReadOrderSnapshot(orders=table, version=retained_version).run(session).snapshot
```

The return annotation must describe the schema at the selected version. `timestamp=` is an alternative to `version=`;
supply exactly one selector. Snapshot reads do not change the current table.

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


class LayoutOrder(Schema):
    customer_id = string(nullable=False)
    product_id = string(nullable=False)
    order_date = string(nullable=False)


class CompactOrders(Transform):
    orders = delta_table(LayoutOrder)

    def compact(self, order: LayoutOrder) -> None:
        delta_optimize(order).execute_compaction()


class ZOrderOrders(Transform):
    orders = delta_table(LayoutOrder)

    def optimize(self, order: LayoutOrder) -> None:
        delta_optimize(order).execute_zorder(by=(order.customer_id, order.product_id))


class ZOrderPartition(Transform):
    orders = delta_table(LayoutOrder)
    selected_date = variable(str)

    def optimize(self, order: LayoutOrder) -> None:
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
    layout_table = DeltaTable.forPath(spark, table_path)

    # Full-table Z-ordering; this transform does not run compaction as another step.
    result = ZOrderOrders(orders=layout_table).run(session)
    assert result.orders is layout_table

    # Alternatively, select a partition with a runtime value.
    scoped = ZOrderPartition(orders=layout_table, selected_date="2026-10-08").run(session)
    assert scoped.orders is layout_table

    # Invoke CompactOrders(orders=layout_table).run(session) separately when compaction is wanted.
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

For a separate caller-owned table matching `Order` with active clustering keys, declare full reclustering explicitly:

```python
class ReclusterOrders(Transform):
    orders = delta_table(Order)

    def optimize(self, order: Order) -> None:
        delta_optimize(order).full()
```

Invoke this transform with that clustered table's handle. Structure does not configure its keys or convert the
disposable partitioned example into a clustered table.

## Restore and vacuum

Restore and vacuum remain independently invoked maintenance effects. These transforms use the original `Order`
schema and caller-owned table:

```python
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

Invoke `RestoreOrders(orders=table, version=retained_version).run(session)` to restore a selected version whose schema
matches `Order`. A cross-schema restore requires separate current and result declarations; see the
[maintenance reference](../reference/DeltaIceberg.ref.md#history-detail-and-maintenance).

Restore creates a new commit. Optimize rewrites files while preserving logical rows. Their native metric DataFrames are
not exposed as transform outputs. An optimize predicate may use only table partition columns. Vacuum defaults to 168
hours; a shorter retention requires `allow_short_retention=True`, and Delta's native retention safety check remains
active. Vacuum removes files that older snapshots may need, so ensure no reader or stream still depends on them.

Invoke `VacuumOrders(orders=table).run(session)` when those retention conditions are met. Each maintenance call commits
independently; Structure does not roll back earlier successes or retry uncertain commits.

See [Delta Tables API](../api/DeltaIceberg.api.md) for table metadata declarations and
[compatibility](../compatibility/DeltaIceberg.compat.md) for the runtime admission status.
