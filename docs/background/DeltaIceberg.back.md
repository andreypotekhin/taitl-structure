# Delta and Iceberg Tables Background

Delta and Iceberg tables are persistent provider-managed state. Structure's ordinary DataFrame outputs describe lazy
relations; a table mutation step instead records a typed effect that commits to an existing table during `run()`. The
compiler and generator can inspect each effect before it reaches the provider API. Callers retain ownership of table
creation, catalog configuration, credentials, and storage lifecycle.

The examples below use two row schemas. The caller supplies `spark`, a `StructureSession` named `session`, source
DataFrames, and existing tables where an invocation is shown:

```python
from structure import *
from structure.plugin.pyspark import *


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class Change(Schema):
    id = string(nullable=False)
    status = string(nullable=False)
```

## Delta tables

The caller creates the table, provisions native CHECK constraints, configures a Delta-capable Spark session, and
passes the native `DeltaTable` handle. Structure binds it through `delta_table(Schema)` for same-schema reads and
mutations, `delta_input(Schema)` for read-only roles, and `delta_output(Schema)` for explicitly evolved result schemas.
A DataFrame cannot stand in for a Delta binding. These helpers are admitted on classic PySpark 3.5, 4.0, and 4.1 with
the pinned Delta pairs listed in the compatibility ledger, and on the exact Spark Connect 4.1 Delta package. PySpark
4.2 and other Connect profiles remain outside this admission. See
[Delta compatibility](../compatibility/DeltaIceberg.compat.md) for the admission status.

### Relations and effects

A `delta_table` is the common relation for reads and same-schema mutations. A typed method may return `None`, with its
unique mutation target inferred from relation resolution, or return a merge operation directly and annotate the method
with the table schema. `@step` disambiguates relations when necessary. A `delta_input` is read-only for row mutations;
it may be a restore target when the return annotation selects a `delta_output` schema. A `delta_output` declares the
result schema for an explicit schema transition and is never a caller-bound table. Relation declarations supply
typed parameters to step methods, so predicates and assignments can refer to typed fields. Successful execution returns
the original caller-provided table handle, not a DataFrame or a metric row.

For example, a transform can apply an update and merge source rows in order:

```python
class ApplyChanges(Transform):
    changes = input(Change)
    orders = delta_table(Order)

    def apply(self, change: Change, order: Order) -> None:
        delta_update(order, where=order.id == "legacy", set=Order(status="pending"))
        (
            delta_merge(order, change, on=order.id == change.id)
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )


result = ApplyChanges(changes=changes_df, orders=delta_table_handle).run(
    StructureSession(spark=spark)
)
assert result.orders is delta_table_handle
```

The invocation supplies the native handle. The method's typed relation parameters describe its schema and expose
fields to Structure's compiler; they do not replace the table object.

Structure checks the current table's columns, types, and nullability before the first mutation. An evolving output is
checked after its commit.

`delta_replace_where(...).execute()` writes a same-schema source into the target's selected slice using Delta's
native `replaceWhere` option. The source and predicate are validated before the commit; Delta enforces that source
rows satisfy the predicate. This operation is a native commit, not a transaction spanning multiple transform steps.

### A deliberate schema transition

A same-schema row-write step cannot silently add columns. To evolve its expected table shape, bind the current table as
`delta_input(OrderV1)`, declare `delta_output(OrderV2)`, annotate the step `-> OrderV2`, and return one
`delta_merge(...).with_schema_evolution(to=OrderV2).execute()` or
`delta_append(...).with_schema_evolution(to=OrderV2).execute()` result. The required `to` schema drives static
compatibility checks and must match the declared output. A `delta_table(OrderV1)` binding may instead target an
evolving effect in a `-> None` step without exposing a separate composable output. Structure validates `OrderV1` before
mutation and `OrderV2` after the native commit. Merge uses Delta's `withSchemaEvolution()`; append applies
`mergeSchema=true` to that writer. The choice is local to the operation, so unrelated writes do not inherit an
auto-merge setting.

Here both the incoming rows and the evolved table add a nullable `note` field:

```python
class ChangeV2(Change):
    note = string()


class OrderV2(Order):
    note = string()


class EvolvingMerge(Transform):
    changes = input(ChangeV2)
    current_orders = delta_input(Order)
    orders = delta_output(OrderV2)

    def merge(self, change: ChangeV2, order: Order) -> OrderV2:
        return (
            delta_merge(order, change, on=order.id == change.id)
            .with_schema_evolution(to=OrderV2)
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )


result = EvolvingMerge(changes=changes_v2_df, current_orders=delta_table_handle).run(session)
assert result.orders is delta_table_handle
```

For later invocations the table has the `OrderV2` schema. Consumers of this transform's table output must declare that
new schema too.

### Historical reads

`delta_snapshot` turns a typed step result into a native historical reader. The caller still provisions and binds
the table. Version or timestamp selectors may be `variable()` values, so the same compiled transform can serve
multiple requests without embedding selectors in generated code. Retention limits the available historical states.

For a versioned snapshot, the output schema describes the table at the selected version:

```python
class ReadSnapshot(Transform):
    orders = delta_input(Order)
    version = variable(int)
    snapshot = output(Order)

    def read(self, order: Order) -> Order:
        return delta_snapshot(order, version=self.version)


snapshot = ReadSnapshot(orders=delta_table_handle, version=18).run(session).snapshot
```

### Change data feed

Delta CDF describes changes committed after the caller enabled the feed. Batch reads use `delta_changes`; streaming
reads use a caller-created DataFrame. Both require a functional Delta Spark session and the table property shown
below. Structure checks the property and Delta extension/catalog settings before a batch CDF read. These preflight
checks can be disabled with `delta_cdf_checks=False`, without enabling CDF or changing Delta behavior. See the
[CDF reference](../reference/DeltaIceberg.ref.md#change-data-feed) for configuration and selector options.

#### Batch CDF

CDF adds metadata columns to the table's row fields. Aliases preserve the native Delta names while providing ordinary
Python attribute names in typed steps:

```python
class OrderChange(Order):
    change_type = string(nullable=False, alias="_change_type")
    commit_version = long(nullable=False, alias="_commit_version")
    commit_timestamp = timestamp(nullable=False, alias="_commit_timestamp")


class ReadOrderChanges(Transform):
    orders = delta_input(Order)
    starting_version = variable(int)
    ending_version = variable(int | None, default=None)
    changes = output(OrderChange)

    def read(self, order: Order) -> OrderChange:
        return delta_changes(
            order,
            starting_version=self.starting_version,
            ending_version=self.ending_version,
        )


changes = ReadOrderChanges(
    orders=delta_table_handle, starting_version=18, ending_version=24
).run(session).changes
```

The caller enables CDF before the changes to be read are committed:

```sql
ALTER TABLE delta.`/path/to/orders`
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

#### Streaming CDF

Streaming CDF stays at the normal DataFrame boundary: the caller builds a `readStream` Delta DataFrame with
`readChangeFeed=true`, binds it to a streaming `input(Schema, streaming=True)`, and owns `writeStream`, checkpoints,
and query lifecycle. Structure only compiles the row transformation.

```python
@transform(streaming=True)
class SelectOrderChanges(Transform):
    changes = input(OrderChange, streaming=True)
    updates = output(OrderChange)

    def select(self, change: OrderChange) -> OrderChange:
        where(change.change_type == "update_postimage")
        return change


stream = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 18)
    .load(orders_path)
)
updates = SelectOrderChanges(changes=stream).run(session).updates
query = updates.writeStream.format("parquet").option(
    "checkpointLocation", checkpoint_path
).start(updates_path)
```

Returning `change` preserves its fields and CDF metadata after the filter. The caller starts and stops `query` and
chooses its sink and checkpoint location.

CDF range endpoints are inclusive. History retention limits the versions and timestamps still available to readers.

### CHECK constraints

A schema's `constraints = (check(...),)` declares the native CHECK metadata that the table must already contain.
Structure verifies the declarations before a write; Delta enforces the predicates on the rows it writes. Declaring a
constraint does not install it:

```python
class CheckedOrder(Schema):
    id = string(nullable=False)
    status = string(nullable=False)
    total = long(nullable=False)
    constraints = (
        check(status != "invalid", name="valid_status"),
        check(total >= 0, name="nonnegative_total"),
    )
```

The caller creates the matching native constraints. For example:

```sql
ALTER TABLE delta.`/path/to/orders`
ADD CONSTRAINT valid_status CHECK (status <> 'invalid');
ALTER TABLE delta.`/path/to/orders`
ADD CONSTRAINT nonnegative_total CHECK (total >= 0);
```

By default, `delta_check_match="expression"` compares normalized predicates. `"name"` compares constraint names, and
`"off"` skips CHECK comparison. Shape checks remain active in every mode. Set the option in plugin configuration, on a
transform, or on a step; the closest declaration wins. Name matching is appropriate when native expressions cannot
be normalized and matching names is sufficient for the caller's policy.

### Generated, identity, and default columns

`Schema.delta_columns` records explicit expectations for generated, identity, and default fields. Ordinary PySpark
binding compares them with Delta's own schema metadata. Connect 4.1 uses them for typing and insert omission, then
lets Delta compute values or reject a write. A mismatched declaration can produce different values without a Structure
error on Connect. Delta generated expressions and identity attributes reside in Delta log schema metadata; defaults
are visible through Spark's `CURRENT_DEFAULT` column metadata. These declarations do not install table features or
make any field optional in ordinary DataFrame schemas.

For example, this schema expects the caller's table to compute `total` and supply a default `status` on insert:

```python
class PricedOrder(Schema):
    id = string(nullable=False)
    price = long(nullable=False)
    quantity = long(nullable=False)
    total = long()
    status = string()
    delta_columns = (
        delta_generated(total, as_="price * quantity"),
        delta_default(status, value="open"),
    )
```

The caller provisions the generated expression and column-default feature before binding the table.

### Execution and failure

Compilation and generation do not import Delta or start Spark. At execution, Structure validates the native handle
and runs each planned operation once. An effect step is retained even if no later DataFrame reads its result. Fresh
internal table reads after a mutation see the latest committed snapshot. The returned object remains the original
handle; if its `toDF()` was materialized before a mutation, reopen the table for a fresh view.

Each native mutation may commit separately. Structure does not provide a transaction across steps, roll back earlier
commits after a later failure, or retry an uncertain native commit. The native exception remains catchable and carries
step context. Delta mutation steps are batch operations; streaming source/sink lifecycle remains caller-owned.

For signatures and working examples, use the [Delta API](../api/DeltaIceberg.api.md) and the
[Delta recipes](../recipes/DeltaTableMutations.md). The durable developer [design](../dev/design/DeltaTables.design.md)
and [specification](../dev/specifications/DeltaTables.spec.md) record the compiler and runtime rules.

### Inheritance and composition

Table relations fit the existing transform model when their provider identity is carried independently from row-frame
names. Inheritance keeps a declaration's provider, role, and schema fixed. Method replacement and `super()` use the
same parent-first scheduling as DataFrame transforms, so separate parent and child effects remain visible commits.

For example, a child cleanup can extend the parent's deletion with an update. Calling `super()` schedules the deletion
before the update; omitting it replaces the inherited method with the child's operation:

```python
class Cleanup(Transform):
    orders = delta_table(Order)

    def clean(self, order: Order) -> None:
        delta_delete(order, where=order.status == "cancelled")


class NormalizeOrders(Cleanup):
    def clean(self, order: Order) -> None:
        super().clean(order)
        delta_update(order, where=order.status == "legacy", set=Order(status="pending"))


class Report(Transform):
    orders = delta_input(Order)
    selected = output(Order)

    def select(self, order: Order) -> Order:
        return Order.project(order)


pipeline = NormalizeOrders(orders=delta_table_handle).to(Report())
result = pipeline.run(session)
selected_orders = result.selected
```

Composed stages pass the caller's existing table reference along with a typed row view. Delta keeps its native handle;
Iceberg keeps its catalog identifier. A later stage refreshes and validates its row view after earlier commits, which
lets an explicit additive schema transition feed a consumer that declares the new schema. A DataFrame-to-table adapter,
intermediate persistence, or multi-step transaction would obscure ownership and commit boundaries, so none is implied
by composition. Explicit snapshot helpers remain the way to preserve historical reads.

### Metadata inspection and table maintenance

`delta_history` and `delta_detail` return typed ordinary relations from a Delta binding. A step annotation selects the
declared result fields, which lets transform code consume recent commits or table location/partition metadata without
hard-coding every vendor column.

```python
class OrderCommit(Schema):
    version = long()
    operation = string()


class OrderDetail(Schema):
    format = string()
    location = string()


class InspectOrders(Transform):
    orders = delta_input(Order)
    limit = variable(int, default=5)
    commits = output(OrderCommit)
    details = output(OrderDetail)

    @step(input=orders, output=commits)
    def history(self, order: Order) -> OrderCommit:
        return delta_history(order, limit=self.limit)

    @step(input=orders, output=details)
    def detail(self, order: Order) -> OrderDetail:
        return delta_detail(order)
```

Both methods explicitly select the table input with `@step`, so the detail read does not consume the history result.

Restore, optimize, and vacuum are explicit effect calls. Restore creates a new version; optimize changes physical file
layout; vacuum removes eligible unreferenced files. Structure does not expose native metric rows as transform results.
Vacuum defaults to 168 hours, and a shorter retention requires source opt-in while retaining Delta's native safety
check. Because vacuum can invalidate old time-travel reads, the caller coordinates it with readers and streams. See the
[inspection and maintenance recipe](../recipes/DeltaInspectionAndMaintenance.md).
See [Delta compatibility](../compatibility/DeltaIceberg.compat.md) for the admitted profiles and evidence.

Restoring to a retained version creates a new commit with that version's contents. Vacuum is a separate operation;
running it later may remove files needed for another restore or historical read:

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

#### Choosing Z-order columns

Z-ordering groups rows with related values into the same files. Delta records column statistics, including minimum
and maximum values, and uses them to skip files that cannot match a query predicate. For example, filters on
`customer_id` and `product_id` can benefit from Z-ordering on those columns. This changes physical layout without
changing rows or schema. It provides no ordering guarantee for query results.

Choose a small set of columns frequently used in selective filters, especially columns with many distinct values.
Each additional key reduces locality for the others. Z-ordering is ineffective for data skipping when statistics
are not collected for its keys. Check the table's statistics configuration before running maintenance; the caller
configures statistics collection, and Structure does not enable it or add a preflight check. Native Delta also
validates the requested operation. See [Delta's optimization guidance](https://docs.delta.io/optimizations-oss/).

On a partitioned table, choose non-partition columns as Z-order keys. Partition pruning already selects directories;
Z-ordering improves file skipping within those partitions. The `delta_optimize(..., where=...)` predicate selects
partitions to rewrite and may reference partition columns only. It does not select individual rows for optimization.

Z-ordering shuffles and rewrites files, and later writes can reduce the benefit of the layout. Repeated Z-ordering
can do more work; do not assume the second invocation is free. Run it explicitly when the workload justifies its
cost, with scheduling and statistics configuration controlled by the caller. Compaction addresses small files
without requesting Z-order keys. The
[maintenance examples](../recipes/DeltaInspectionAndMaintenance.md#compaction-and-z-ordering) invoke the operations
separately.

For the example `Order` schema, compaction and Z-ordering can be declared as separate transforms:

```python
class CompactOrders(Transform):
    orders = delta_table(Order)

    def compact(self, order: Order) -> None:
        delta_optimize(order).execute_compaction()


class ZOrderOrders(Transform):
    orders = delta_table(Order)

    def optimize(self, order: Order) -> None:
        delta_optimize(order).execute_zorder(by=(order.id,))
```

The Z-order example assumes `id` is not a partition column and has statistics collected. Each invocation performs the
declared maintenance; the caller chooses when to run it.

#### Liquid clustering

Liquid clustering is a different table layout and cannot be combined with Z-ordering on the same table. Choose
maintenance appropriate to the existing table; Structure does not convert its layout. See
[Delta liquid clustering](https://docs.delta.io/delta-clustering/) and the
[operation reference](../reference/DeltaIceberg.ref.md#liquid-clustering).

Liquid-clustered tables retain their caller-configured keys: `execute_compaction()` invokes Delta's incremental
clustering, while `delta_optimize(...).full()` reclusters existing data against active keys. Typed `delta_detail`
results can include Delta's `clusteringColumns` field. On admitted open-source profiles, clustered tables reject
optimization predicates and Z-order, including after clustering keys are cleared; Databricks partial reclustering is
outside this admission.

On a table with active liquid-clustering keys, request full reclustering instead:

```python
class ReclusterOrders(Transform):
    orders = delta_table(Order)

    def optimize(self, order: Order) -> None:
        delta_optimize(order).full()
```

## Apache Iceberg tables

Iceberg tables are caller-owned format-v2 catalog objects addressed by names such as `warehouse.sales.orders`.
Structure binds those names through `iceberg_input(Schema)`, `iceberg_table(Schema)`, or `iceberg_output(Schema)`.
The table name is a runtime input to a compiled transform, and an Iceberg table output returns that same identifier.
The caller provides
the Spark session, runtime jars, catalog configuration, credentials, and table lifecycle. Structure does not create
catalogs or bridge Iceberg metadata through PyIceberg.

The first-class SQL path is the existing typed `sql(...)` helper. It remains the complete escape hatch for supported
Iceberg SQL and procedures; convenience helpers cover common mutations, reads, and maintenance while retaining
native SQL semantics. Passing a table binding through `relations` resolves its runtime identifier, while DataFrame
relations continue to bind row scopes. Generic SQL does not imply helper admission for every provider operation.

The caller configures the catalog and provides its table name at invocation. A typed binding can serve as a runtime
SQL relation or as the target of a helper:

```python
class ReadOpenOrders(Transform):
    orders = iceberg_table(Order)
    open_orders = output(Order)

    def read(self, order: Order) -> Order:
        return sql(
            "SELECT id, status FROM {order} WHERE status = :status",
            relations={"order": order},
            args={"status": "open"},
            to=Order,
        )


result = ReadOpenOrders(orders="warehouse.sales.orders").run(session)
open_orders = result.open_orders
```

Here the table identifier stays an invocation value and the predicate value is bound as a SQL argument. SQL `CREATE`,
`ALTER`, `INSERT`, `MERGE`, `CALL`, and other supported statements remain available through the same helper when their
native result and effect semantics are appropriate.

`iceberg_table` supports reads and same-schema effects. `iceberg_input` is read-only except when it is the source
binding for an explicitly evolving append. `iceberg_output` names the result schema for that transition. Append,
update, delete, merge, snapshot reads, metadata inspection, rollback, rewrite, and cleanup are explicit typed
operations. Each write or maintenance call is an individual native effect: Structure does not provide a transaction
across steps, automatically retry a commit, or undo earlier commits after a later failure.

For example, the merge helper updates matching rows and inserts new ones using the typed source fields:

```python
class MergeOrders(Transform):
    changes = input(Change)
    orders = iceberg_table(Order)

    def merge(self, change: Change, order: Order) -> None:
        (
            iceberg_merge(order, change, on=order.id == change.id)
            .when_matched_update(set=Order(status=change.status))
            .when_not_matched_insert(values=Order(id=change.id, status=change.status))
            .execute()
        )
```

The admitted helper lanes use Iceberg format v2 and Iceberg 1.12.0 on Spark 3.5.3, 4.0.0, and 4.1.0 classic, plus
Spark Connect 4.1.0. The Connect server owns its Iceberg runtime and catalog configuration. See the combined
[compatibility ledger](../compatibility/DeltaIceberg.compat.md#apache-iceberg) and
[Iceberg API](../api/DeltaIceberg.api.md#apache-iceberg) for the exact operation scope. The separate developer
[Iceberg design](../dev/design/IcebergTables.design.md) and
[Iceberg specification](../dev/specifications/IcebergTables.spec.md) document compiler and runtime contracts.

### Evolving append

Schema evolution is opt-in on one append and produces a distinct declared result schema:

```python
class AddNotes(Transform):
    updates = input(OrderV2)
    current = iceberg_input(Order)
    evolved = iceberg_output(OrderV2)

    def append(self, update: OrderV2, order: Order) -> OrderV2:
        return iceberg_append(order, update).with_schema_evolution(to=OrderV2).execute()


result = AddNotes(updates=updates_df, current="warehouse.sales.orders").run(session)
assert result.evolved == "warehouse.sales.orders"
```

The caller must set `write.spark.accept-any-schema=true` before this operation. Structure does not alter table
properties. Helper evolution supports additive nullable fields; an ordinary append still requires the declared
current schema.

The caller can enable the property explicitly through Spark SQL:

```sql
ALTER TABLE warehouse.sales.orders
SET TBLPROPERTIES ('write.spark.accept-any-schema' = 'true')
```

Structure validates the current schema before appending and the declared evolved schema after the commit.

### Historical reads

Historical reads return ordinary typed DataFrames. Snapshot IDs are opaque native identifiers, with no sequential
Delta version relationship. The selected snapshot must be retained, and the result annotation describes its row
schema. Use exactly one of `snapshot_id=` or `timestamp=`; selectors may be runtime `variable()` values. Timestamp
selectors use timezone-aware `datetime` values.

This historical read selects a retained snapshot whose rows have the original `Order` schema:

```python
class ReadIcebergSnapshot(Transform):
    orders = iceberg_input(Order)
    snapshot_id = variable(int)
    snapshot = output(Order)

    def read(self, order: Order) -> Order:
        return iceberg_snapshot(order, snapshot_id=self.snapshot_id)
```

### Inheritance and composition

Inheritance and composition follow the same rules shown for Delta. A later stage can consume the committed Iceberg
table by receiving its catalog identifier:

```python
class ReportIcebergOrders(Transform):
    orders = iceberg_input(Order)
    selected = output(Order)

    def select(self, order: Order) -> Order:
        return Order.project(order)


pipeline = MergeOrders(changes=changes_df, orders="warehouse.sales.orders").to(ReportIcebergOrders())
selected_orders = pipeline.run(session).selected
```

### Metadata inspection

`iceberg_history` reads changes to the current snapshot; `iceberg_snapshots` reads snapshot operations and commit
times. Their native schemas differ: history contains `made_current_at` and `is_current_ancestor`, while snapshots
contains `committed_at` and `operation`. Declare the fields needed from the chosen relation. For example:

```python
class Commit(Schema):
    snapshot_id = long()
    made_current_at = timestamp()


class ReadCommits(Transform):
    orders = iceberg_input(Order)
    limit = variable(int, default=5)
    commits = output(Commit)

    def history(self, order: Order) -> Commit:
        return iceberg_history(order, limit=self.limit)
```

History is newest first by `made_current_at`; snapshots are newest first by `committed_at`. Both helpers accept
`limit=None` or a positive integer, including a runtime variable. `iceberg_metadata(..., kind=..., to=Schema)` reads a
typed subset of files, manifests, partitions, refs, history, or snapshots metadata.

For a metadata read, declare the fields needed from the chosen native metadata table:

```python
class DataFile(Schema):
    file_path = string()
    record_count = long()


class InspectFiles(Transform):
    orders = iceberg_input(Order)
    files = output(DataFile)

    def inspect(self, order: Order) -> DataFile:
        return iceberg_metadata(order, kind="files", to=DataFile)
```

Snapshot rows and metadata rows are ordinary DataFrame results. They do not replace the caller's catalog binding.

### Maintenance and effects

Rollback, rewriting, expiration, and orphan cleanup are independent native procedure calls. Rollback follows native
ancestry rules and requires a retained snapshot. For example:

```python
class RollbackOrders(Transform):
    orders = iceberg_table(Order)
    snapshot_id = variable(int)

    def rollback(self, order: Order) -> None:
        iceberg_rollback(order, snapshot_id=self.snapshot_id).execute()
```

Use `iceberg_rewrite_data_files`, `iceberg_rewrite_manifests`, `iceberg_expire_snapshots`, and
`iceberg_remove_orphan_files` for their corresponding operations. When a procedure's native metrics are needed, call it
through `sql(..., to=ResultSchema)` with the result schema declared by the pinned runtime. Retention guards remain
enabled; exercise destructive cleanup against a disposable catalog before applying it to caller-owned data.

#### File and manifest rewrites

File and manifest rewrites can be scheduled together, with each procedure retaining its own commit:

```python
class RewriteOrders(Transform):
    orders = iceberg_table(Order)

    def rewrite(self, order: Order) -> None:
        iceberg_rewrite_data_files(order, strategy="binpack").execute()
        iceberg_rewrite_manifests(order).execute()
```

#### Snapshot expiration and orphan cleanup

Retention maintenance is separate. Supply a timezone-aware `datetime` as `cutoff` and choose a retention window that
leaves files needed by active readers and writers available:

```python
from datetime import datetime


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

The orphan-file example requests a dry run. To inspect its candidate files or other procedure metrics, declare the
native result schema and invoke the procedure through `sql(...)` as described above.

### Choosing SQL or a helper

Use a typed helper when its signature captures the intended operation and Structure can validate the binding, schema,
and arguments at compile time. Use `sql(...)` for table creation, catalog administration, branches and tags, uncommon
procedure options, and future SQL features not covered by helpers. SQL is the complete supported Spark interface at
the boundary; the helper catalog remains deliberately narrower and Iceberg-specific.
