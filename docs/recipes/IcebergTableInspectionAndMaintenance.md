# Inspect and Maintain an Iceberg Table in Transforms

**Problem:** An existing orders table needs its snapshot history inspected, files reorganized, or retained data cleaned
up without hiding those operations inside ordinary row transformations.

**Solution:** Return typed metadata relations and invoke rollback, rewrites, and cleanup as separate effect transforms.

The caller supplies an Iceberg-capable Spark session and an existing format-v2 table named `warehouse.sales.orders`.
The examples use the same table identifier and row schema throughout:

```python
from datetime import datetime, timezone

from structure import *
from structure.plugin.pyspark import *


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


session = StructureSession(spark=spark)
table_name = "warehouse.sales.orders"
```

## Inspect history, snapshots, and files

Iceberg history records changes to the current snapshot; snapshots metadata records operations and commit times.
They have different native columns. Declare the fields needed from each relation:

```python
class HistoryEntry(Schema):
    snapshot_id = long()
    made_current_at = timestamp()
    is_current_ancestor = boolean()


class SnapshotEntry(Schema):
    snapshot_id = long()
    committed_at = timestamp()
    operation = string()


class DataFile(Schema):
    file_path = string()
    record_count = long()


class InspectOrders(Transform):
    orders = iceberg_input(Order)
    limit = variable(int, default=10)
    history_rows = output(HistoryEntry)
    snapshot_rows = output(SnapshotEntry)
    files = output(DataFile)

    @step(input=orders, output=history_rows)
    def history(self, order: Order) -> HistoryEntry:
        return iceberg_history(order, limit=self.limit)

    @step(input=orders, output=snapshot_rows)
    def snapshots(self, order: Order) -> SnapshotEntry:
        return iceberg_snapshots(order, limit=self.limit)

    @step(input=orders, output=files)
    def data_files(self, order: Order) -> DataFile:
        return iceberg_metadata(order, kind="files", to=DataFile)


inspection = InspectOrders(orders=table_name, limit=5).run(session)
recent_snapshots = inspection.snapshot_rows
current_files = inspection.files
```

The explicit `@step` bindings make each method read the original table input. History is newest first by
`made_current_at`, and snapshots by `committed_at`. A supplied limit must be positive. Other metadata kinds include
`manifests`, `partitions`, and `refs`; use their native fields in the declared return schema.

## Read a retained snapshot

Use a native snapshot ID selected from metadata. Snapshot IDs are opaque identifiers, not sequential version numbers:

```python
class ReadOrderSnapshot(Transform):
    orders = iceberg_input(Order)
    snapshot_id = variable(int)
    snapshot = output(Order)

    def read(self, order: Order) -> Order:
        return iceberg_snapshot(order, snapshot_id=self.snapshot_id)


prior_orders = ReadOrderSnapshot(
    orders=table_name, snapshot_id=retained_snapshot_id
).run(session).snapshot
```

The return annotation must describe the schema at the selected snapshot. `timestamp=` accepts a timezone-aware
`datetime` as an alternative to `snapshot_id=`; supply exactly one selector.

## Rewrite data files and manifests

Run file layout maintenance independently from ingestion. These two transforms let the caller choose each operation:

```python
class RewriteOrderFiles(Transform):
    orders = iceberg_table(Order)

    def rewrite(self, order: Order) -> None:
        iceberg_rewrite_data_files(order, strategy="binpack").execute()


class RewriteOrderManifests(Transform):
    orders = iceberg_table(Order)

    def rewrite(self, order: Order) -> None:
        iceberg_rewrite_manifests(order, use_caching=True).execute()


result = RewriteOrderFiles(orders=table_name).run(session)
assert result.orders == table_name
```

Data-file rewriting also accepts `sort_order=`, a typed target-only `where=` predicate, and an `options=` mapping of
strings to strings. Manifest rewriting accepts a nonnegative `spec_id=`. Native Iceberg validates the requested
strategy and procedure options. The helpers do not return procedure metric DataFrames.

## Roll back to an ancestor snapshot

Rollback changes the table's current snapshot and follows Iceberg's native ancestry rules:

```python
class RollbackOrders(Transform):
    orders = iceberg_table(Order)
    snapshot_id = variable(int)

    def rollback(self, order: Order) -> None:
        iceberg_rollback(order, snapshot_id=self.snapshot_id).execute()
```

Invoke this transform explicitly with a retained ancestor ID. A timezone-aware `timestamp=` selector is also
available. Rollback requires files from the selected snapshot to remain available.

## Expire snapshots and check orphan files

Choose a retention cutoff after accounting for active readers, writers, and replay needs. Pass a timezone-aware
`datetime`; `retain_last` is a positive integer literal:

```python
class ExpireOrderSnapshots(Transform):
    orders = iceberg_table(Order)
    cutoff = variable(datetime)

    def expire(self, order: Order) -> None:
        iceberg_expire_snapshots(order, older_than=self.cutoff, retain_last=5).execute()


class CheckOrphanFiles(Transform):
    orders = iceberg_table(Order)
    cutoff = variable(datetime)

    def check(self, order: Order) -> None:
        iceberg_remove_orphan_files(order, older_than=self.cutoff, dry_run=True).execute()
```

Supply `cutoff` at invocation, for example `datetime(2026, 1, 1, tzinfo=timezone.utc)` if that date fits the caller's
retention policy. Expiration retains Iceberg's native reference safeguards. The orphan-file example requests a dry
run; use native SQL with a declared result schema to inspect candidates before choosing deletion.

Each maintenance call commits independently. Structure does not combine them into a transaction or retry uncertain
commits. Cleanup can make historical reads unavailable, so the caller chooses when to run it. For procedure metrics,
use `sql(..., to=ResultSchema)` with the native result fields.

See the [Iceberg reference](../reference/DeltaIceberg.ref.md#history-snapshots-and-metadata) for metadata signatures,
the [maintenance reference](../reference/DeltaIceberg.ref.md#rollback-and-file-maintenance), and the
[compatibility ledger](../compatibility/DeltaIceberg.compat.md#apache-iceberg).
