# Delta and Iceberg Tables Background

Delta and Iceberg tables are persistent provider-managed state. Structure's ordinary DataFrame outputs describe lazy
relations; a table mutation step instead records a typed effect that commits to an existing table during `run()`. The
compiler and generator can inspect each effect before it reaches the provider API. Callers retain ownership of table
creation, catalog configuration, credentials, and storage lifecycle.

## Delta tables

The caller creates the table, provisions native CHECK constraints, configures a Delta-capable Spark session, and
passes the native `DeltaTable` handle. Structure binds it through `delta_table(Schema)` for same-schema reads and
mutations, `delta_input(Schema)` for read-only roles, and `delta_output(Schema)` for explicitly evolved result schemas.
A DataFrame cannot stand in for a Delta binding. These helpers are admitted on classic PySpark 3.5, 4.0, and 4.1 with
the pinned Delta pairs listed in the compatibility ledger, and on the exact Spark Connect 4.1 Delta package. PySpark
4.2 and other Connect profiles remain outside this admission. See [Delta compatibility](../compatibility/DeltaIceberg.compat.md)
for the admission status.

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
from structure import Schema, StructureSession, Transform, input
from structure.plugin.pyspark import delta_merge, delta_table, delta_update, string


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class Change(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class ApplyChanges(Transform):
    changes = input(Change)
    orders = delta_table(Order)

    def apply(self, change: Change, order: Order) -> None:
        delta_update(order, where=order.id == "legacy", set=Order(status="pending"))
        (delta_merge(order, change, on=order.id == change.id)
         .when_matched_update_all()
         .when_not_matched_insert_all()
         .execute())


result = ApplyChanges(changes=changes_df, orders=delta_table_handle).run(
    StructureSession(spark=spark)
)
assert result.orders is delta_table_handle
```

The invocation supplies the native handle. The method's typed relation parameters describe its schema and expose
fields to Structure's compiler; they do not replace the table object.

Structure checks the current table's columns, types, nullability, and declared CHECKs before the first mutation. An
evolving output is checked after its commit. A schema's `constraints = (check(...),)` states what native CHECK metadata
the table must already contain. It is not a request to install that metadata. By default Structure compares normalized
predicates; `delta_check_match="name"`
compares names, and `"off"` skips CHECK comparison. Shape checks remain active. The option can be specified in plugin
configuration, on a transform, or on a step; the closest declaration wins.

### A deliberate schema transition

A same-schema row-write step cannot silently add columns. To evolve its expected table shape, bind the current table as
`delta_input(OrderV1)`, declare `delta_output(OrderV2)`, annotate the step `-> OrderV2`, and return one
`delta_merge(...).with_schema_evolution(to=OrderV2).execute()` or
`delta_append(...).with_schema_evolution(to=OrderV2).execute()` result. The required `to` schema drives static
compatibility checks and must match the declared output. A `delta_table(OrderV1)` binding may instead target an
evolving effect in a `-> None` step without exposing a separate composable output. Structure validates `OrderV1` before mutation and
`OrderV2` after the native commit. Merge uses Delta's `withSchemaEvolution()`; append applies `mergeSchema=true` to
that writer. The choice is local to the operation, so unrelated writes do not inherit an auto-merge setting.

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

### Historical reads, CDF, and selective overwrite

`delta_snapshot` and `delta_changes` turn a typed step result into a native Delta reader. The caller still provisions
and binds the table. Snapshot version/timestamp and CDF start/end selectors may be `variable()` values, so the same
compiled transform can serve multiple requests without embedding selector values in generated code. Batch CDF needs
the table property `delta.enableChangeDataFeed=true` and the Spark Delta extension/catalog settings. Structure checks
both before opening the feed. CDF endpoints are inclusive, and Delta history retention still limits which ranges can
be read.

Streaming CDF stays at the normal DataFrame boundary: the caller builds a `readStream` Delta DataFrame with
`readChangeFeed=true`, binds it to a streaming `input(Schema, streaming=True)`, and owns `writeStream`, checkpoints,
and query lifecycle. Structure only compiles the row transformation.

`delta_replace_where(...).execute()` writes a same-schema source into the target's selected slice using Delta's
native `replaceWhere` option. The source and predicate are validated before the commit; Delta enforces that source
rows satisfy the predicate. This operation is a native commit, not a transaction spanning multiple transform steps.

### Metadata inspection and table maintenance

`delta_history` and `delta_detail` return typed ordinary relations from a Delta binding. A step annotation selects the
declared result fields, which lets transform code consume recent commits or table location/partition metadata without
hard-coding every vendor column.

`Schema.delta_columns` records explicit expectations for generated, identity, and default fields. Ordinary PySpark
binding compares them with Delta's own schema metadata. Connect 4.1 uses them for typing and insert omission, then
lets Delta compute values or reject a write. A mismatched declaration can produce different values without a Structure
error on Connect. Delta generated expressions and identity attributes reside in Delta log schema metadata; defaults
are visible through Spark's `CURRENT_DEFAULT` column metadata. These declarations do not install table features or
make any field optional in ordinary DataFrame schemas.

Restore, optimize, and vacuum are explicit effect calls. Restore creates a new version; optimize changes physical file
layout; vacuum removes eligible unreferenced files. Structure does not expose native metric rows as transform results.
Vacuum defaults to 168 hours, and a shorter retention requires source opt-in while retaining Delta's native safety
check. Because vacuum can invalidate old time-travel reads, the caller coordinates it with readers and streams. See the
[inspection and maintenance recipe](../recipes/DeltaInspectionAndMaintenance.md).
See [Delta compatibility](../compatibility/DeltaIceberg.compat.md) for the admitted profiles and evidence.

## Apache Iceberg tables

Iceberg tables are caller-owned catalog objects addressed by names such as `warehouse.sales.orders`. Structure binds
those names through `iceberg_input(Schema)`, `iceberg_table(Schema)`, or `iceberg_output(Schema)`. The table name is a
runtime input to a compiled transform, and an Iceberg table output returns that same identifier. The caller provides
the Spark session, runtime jars, catalog configuration, credentials, and table lifecycle. Structure does not create
catalogs or bridge Iceberg metadata through PyIceberg.

The first-class SQL path is the existing typed `sql(...)` helper. It remains the complete escape hatch for supported
Iceberg SQL and procedures; convenience helpers cover common mutations, reads, and maintenance while retaining
native SQL semantics. Passing a table binding through `relations` resolves its runtime identifier, while DataFrame
relations continue to bind row scopes. Generic SQL does not imply helper admission for every provider operation.

The caller configures the catalog and provides its table name at invocation. A typed binding can serve as a runtime
SQL relation or as the target of a helper:

```python
from structure import Schema, Transform, output
from structure.plugin.pyspark import iceberg_table, sql, string


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


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

Iceberg snapshot identifiers are opaque native IDs, with no implied sequential Delta version relationship. Historical
reads require retained snapshots; rollback follows native ancestry rules. Schema evolution is opt-in per append and
requires the caller's `write.spark.accept-any-schema=true` table property. Structure applies the writer merge option
only to that append and does not alter the property. The initial helper contract supports additive nullable columns.
Maintenance keeps Iceberg's own retention and reference safeguards; orphan deletion and snapshot expiration should
be scoped to the caller's intended catalog and warehouse.

The admitted helper lanes use Iceberg format v2 and Iceberg 1.12.0 on Spark 3.5.3, 4.0.0, and 4.1.0 classic, plus
Spark Connect 4.1.0. The Connect server owns its Iceberg runtime and catalog configuration. See the combined
[compatibility ledger](../compatibility/DeltaIceberg.compat.md#apache-iceberg) and
[Iceberg API](../api/DeltaIceberg.api.md#apache-iceberg-tables) for the exact operation scope. The separate developer
[Iceberg design](../dev/design/IcebergTables.design.md) and
[Iceberg specification](../dev/specifications/IcebergTables.spec.md) document compiler and runtime contracts.

### Evolving append and historical reads

Schema evolution is opt-in on one append and produces a distinct declared result schema:

```python
from structure import Schema, Transform, input
from structure.plugin.pyspark import iceberg_append, iceberg_input, iceberg_output, string


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class OrderV2(Order):
    note = string()


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

Historical reads return ordinary typed DataFrames. Snapshot IDs are opaque Iceberg values, and the result annotation
declares the selected historical schema. The same selector may be supplied by a runtime `variable(...)` when one
compiled transform serves multiple snapshots.

```python
from structure import Schema, Transform, output, variable
from structure.plugin.pyspark import iceberg_history, iceberg_input, long, string


class Commit(Schema):
    snapshot_id = long()
    operation = string()


class ReadCommits(Transform):
    orders = iceberg_input(Order)
    limit = variable(int, default=5)
    commits = output(Commit)

    def history(self, order: Order) -> Commit:
        return iceberg_history(order, limit=self.limit)
```

For snapshot contents, use `iceberg_snapshot(order, snapshot_id=...)` or `iceberg_snapshot(order, timestamp=...)` in a
step whose return schema matches the table at that point. Use `iceberg_metadata(..., kind=..., to=Schema)` for a typed
subset of files, manifests, partitions, refs, history, or snapshots metadata.

### Maintenance and effects

Rollback, rewriting, expiration, and orphan cleanup are independent native procedure calls. For example:

```python
from structure import Transform, variable
from structure.plugin.pyspark import iceberg_rollback, iceberg_table


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

### Choosing SQL or a helper

Use a typed helper when its signature captures the intended operation and Structure can validate the binding, schema,
and arguments at compile time. Use `sql(...)` for table creation, catalog administration, branches and tags, uncommon
procedure options, and future SQL features not covered by helpers. SQL is the complete supported Spark interface at
the boundary; the helper catalog remains deliberately narrower and Iceberg-specific.
