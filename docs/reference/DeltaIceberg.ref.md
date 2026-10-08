# Delta and Iceberg Tables Reference

Structure provides typed reads, mutations, and maintenance for existing Delta and Apache Iceberg tables. The caller
creates the tables and supplies the provider runtime, Spark session, credentials, and storage configuration. Provider
support is optional and admitted for the exact profiles in the
[compatibility ledger](../compatibility/DeltaIceberg.compat.md).

## Delta tables

Delta bindings use a native `DeltaTable` handle. Classic PySpark 3.5, 4.0, and 4.1 and the exact Spark Connect 4.1 Delta
package are admitted with the tested Delta pairs in the compatibility ledger. PySpark 4.2 and other Connect profiles
are outside this admission.

### Declarations

The examples use these row schemas and imports. The caller supplies a configured Spark session, a
`StructureSession` named `session`, source DataFrames, and existing tables where invocations are shown:

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

| Structure API | Purpose | Example |
| --- | --- | --- |
| `delta_input(Schema)` | Reads or schema-transition source | `current = delta_input(Order)` |
| `delta_table(Schema)` | Reads and same-schema mutations | `orders = delta_table(Order)` |
| `delta_output(Schema)` | Explicit schema-transition result | `orders = delta_output(OrderV2)` |

Table columns, types, and nullability must match the current declared schema before mutation. For explicit evolution,
the new output schema is checked after the commit. Every Delta binding needs a native `DeltaTable`, not a DataFrame.

### Same-schema mutations

The target is a relation parameter in an effect step. A plain typed method can infer its same-schema mutation target and
return `None`; `@step` can disambiguate relations when a schema is used more than once. Authors who prefer a typed
return may return a merge operation directly and annotate the method with the target schema. Structure checks that this
annotation matches the bound table.

```python
class Apply(Transform):
    changes = input(Change)
    orders = delta_table(Order)

    def apply(self, change: Change, order: Order) -> None:
        delta_delete(order, where=order.id == "2")
        delta_update(order, where=order.id == "1", set=Order(status="pending"))
        (
            delta_merge(order, change, on=order.id == change.id)
            .when_matched_update(set=Order(status=change.status))
            .when_not_matched_insert(values=Order(id=change.id, status=change.status))
            .execute()
        )


result = Apply(changes=changes_df, orders=table).run(StructureSession(spark=spark))
assert result.orders is table
```

For a merge-only step, the mutation result may be the method's typed return value:

```python
class MergeOrders(Transform):
    changes = input(Change)
    orders = delta_table(Order)

    def merge(self, change: Change, order: Order) -> Order:
        return (
            delta_merge(order, change, on=order.id == change.id)
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
```

The return annotation does not turn the table into a DataFrame output. It states that the returned mutation targets a
table with the same `Order` schema, and Structure checks that contract during compilation.

`delta_delete(target, where=...)` and `delta_update(target, where=..., set=Schema(...))` require an explicit Boolean
predicate. `where=True` deliberately affects every row. Update values may specify some target fields; insert values
must supply every non-nullable target field. Predicates and assignments use typed Structure expressions.

`delta_merge(target, source, on=...)` requires a target/source match expression. Its ordered builder supports
`when_matched_update`, `when_matched_delete`, `when_matched_update_all`, `when_not_matched_insert`,
`when_not_matched_insert_all`, `when_not_matched_by_source_update`, and `when_not_matched_by_source_delete`.
Clause methods accept `condition=` where Delta permits it; update and insert methods accept `set=` and `values=`
respectively. Finish the builder with `.execute()`. `delta_append(target, source).execute()` appends a source relation.

`delta_replace_where(target, source, where=...).execute()` performs a same-schema selective overwrite. The predicate
may reference target fields and runtime variables. The source must use the same Structure Schema; Delta also checks
that incoming rows satisfy the predicate. The call is a native commit and is retained as a step effect.

### Explicit schema evolution

For an expected schema change, declare the current table as `delta_input(CurrentSchema)` and the result as
`delta_output(NewSchema)`. Return exactly one merge or append operation with
`.with_schema_evolution(to=NewSchema)`; the explicit `to` schema drives compatibility checks and must match the
declared output. Do not pass the new output as a step parameter or invocation argument.

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


result = EvolvingMerge(changes=changes_v2_df, current_orders=table).run(StructureSession(spark=spark))
assert result.orders is table
```

Merge evolution maps to Delta's `withSchemaEvolution()`. The append form is
`return delta_append(order, change).with_schema_evolution(to=OrderV2).execute()`; it applies `mergeSchema=true` to that
append writer. Neither form changes a session-wide setting. Structure checks the old shape before the commit and the
declared new shape and CHECK metadata afterward. For a later invocation, bind the table using its new schema.

### Snapshot reads

Use `delta_snapshot` as the direct return value of a single-output step. The result annotation declares the schema
at the selected historical point. Supply exactly one of `version=` or `timestamp=`:

```python
class ReadOrderSnapshot(Transform):
    orders = delta_input(Order)
    version = variable(int)
    snapshot = output(Order)

    def read(self, order: Order) -> Order:
        return delta_snapshot(order, version=self.version)


snapshot = ReadOrderSnapshot(orders=table, version=18).run(session).snapshot
```

`variable(type, default=...)` supplies a runtime scalar without specializing the compiled transform for each value.
It can appear in Spark expressions and Delta selectors. It cannot control Python `if` statements or graph shape; use
`parameter()` for compile-time choices. Supported scalar types are `bool`, `int`, `float`, `str`, `bytes`, `Decimal`,
`date`, and timezone-aware `datetime`; `Decimal` declarations require `precision=` and `scale=`. Snapshot retention
limits the versions and timestamps that remain available.

### Change data feed

Use `delta_changes` for batch change-feed reads. Return it directly from a single-output step whose result schema
includes the required row fields and CDF metadata. Native metadata column names are declared through aliases.
`delta_changes` accepts `starting_version=` with optional `ending_version=`, or `starting_timestamp=` with optional
`ending_timestamp=`. Range endpoints are inclusive; timestamp values are timezone-aware `datetime` values.

The caller enables CDF on the table before writing changes:

```sql
ALTER TABLE delta.`/path/to/orders`
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

Before a batch CDF read, Structure checks by default that the table property `delta.enableChangeDataFeed` is `true` and
that both `spark.sql.extensions` and `spark.sql.catalog.spark_catalog` contain `delta`, case-insensitively. These are
sanity checks rather than exact vendor-class checks; Spark and Delta still need to accept the configured classes.
Disable both preflight checks with `delta_cdf_checks=False` in PySpark plugin configuration, `@transform(...)`, or
`@step(...)`; the closest setting wins. For example, set it in `pyproject.toml`:

```toml
[tool.structure.plugin.pyspark]
delta_cdf_checks = false
```

Disabling checks does not enable CDF or change Delta behavior. CDF only includes
changes committed after the property was enabled, and the requested versions or timestamps must still be available in
Delta history.

#### Batch CDF reads

```python
class OrderChange(Schema):
    id = string(nullable=False)
    status = string(nullable=False)
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


first = ReadOrderChanges(orders=table, starting_version=18).run(session).changes
later = ReadOrderChanges(orders=table, starting_version=24).run(session).changes
```

#### Streaming CDF boundary

Structure does not start or manage a streaming CDF reader. Bind the caller-created stream as an ordinary streaming
input, transform it through a normal typed step, then let the caller own `writeStream` and its checkpoint:

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

The table CDF property and Delta Spark session configuration described above also apply. The caller owns query
startup, checkpointing, and shutdown.

### CHECK constraints

Use `Schema.constraints` to declare expected native Delta CHECK constraints. Structure verifies that the bound table
contains matching constraints; it does not install them. Delta enforces the native predicates on rows written by a
mutation. For example, declare a constrained schema and merge rows into a table with that schema:

```python
class CheckedOrder(Schema):
    id = string(nullable=False)
    status = string(nullable=False)
    total = long(nullable=False)
    constraints = (
        check(status != "invalid", name="valid_status"),
        check(total >= 0, name="nonnegative_total"),
    )


class CheckedChange(Change):
    total = long(nullable=False)


class ApplyCheckedOrders(Transform):
    changes = input(CheckedChange)
    orders = delta_table(CheckedOrder)

    def merge(self, change: CheckedChange, order: CheckedOrder) -> None:
        (
            delta_merge(order, change, on=order.id == change.id)
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
```

The caller provisions both constraints when creating the table or adds them explicitly:

```sql
ALTER TABLE delta.`/path/to/orders`
ADD CONSTRAINT valid_status CHECK (status <> 'invalid');
ALTER TABLE delta.`/path/to/orders`
ADD CONSTRAINT nonnegative_total CHECK (total >= 0);
```

`delta_check_match` may be set in PySpark plugin configuration, `@transform(...)`, or `@step(...)`; the nearest setting
wins. The default, `"expression"`, compares native and declared CHECK predicates after normalizing supported SQL
syntax. `"name"` checks names without comparing predicates. `"off"` skips CHECK verification. Table shape checks
remain active in every mode. An unsupported native CHECK expression fails under the default mode; use `"name"` only
when name matching is sufficient for your table policy.

### Generated, identity, and default columns

Declare expected generated, identity, and default columns in `Schema.delta_columns`. These declarations describe
features that the caller has already provisioned on the native table; they do not install them or make fields optional
in ordinary DataFrame schemas:

```python
class GeneratedOrder(Schema):
    id = long()
    price = long(nullable=False)
    quantity = long(nullable=False)
    total = long()
    status = string()
    delta_columns = (
        delta_identity(id, mode="always", start=1, step=1),
        delta_generated(total, as_="price * quantity"),
        delta_default(status, value="open"),
    )
```

The caller provisions the Delta table with those features before binding it. Ordinary PySpark compares declarations
with native feature metadata before a write. An insert may omit a declared generated, identity, or default column; an
undeclared required field still fails. `GENERATED ALWAYS` identity fields cannot be assigned. Delta checks an explicitly
supplied generated value. Identity columns use `long`; Delta identity tables have concurrency restrictions that remain
in force. Default values require Delta's column-default table feature to be enabled by the caller.

### Execution and failure

Delta operations are batch-only. Each operation makes its own native commit; Structure does not combine steps into a
transaction or retry uncertain commits. A later failure does not undo an earlier success. Online and generated modes
run the same checked operations. The returned `DeltaTable` is the caller's original object; if its `toDF()` was
materialized earlier, reopen the table to inspect the latest snapshot.

For rationale and lifecycle details, see [Delta table background](../background/DeltaIceberg.back.md). For recipes, see
[same-schema mutations](../recipes/DeltaTableMutations.md), [schema evolution](../recipes/DeltaSchemaEvolution.md), and
[change data feed](../recipes/DeltaChangeDataFeed.md). For the exact
release and target limits, see [Delta and Iceberg compatibility](../compatibility/DeltaIceberg.compat.md).

### Inheritance and composition

Delta declarations keep their provider, role, and schema through transform inheritance. A child can replace an inherited
table method in place or call `super()` to schedule the parent's complete effect immediately before its own. Composition
can pass the same caller-owned table from a `delta_table` or explicit evolution result to a matching `delta_input` or
`delta_table` stage. Provider and schema mismatches fail at compile time, and a read-only wrapper input cannot be
escalated to a mutable stage input.

```python
class Cleanup(Transform):
    orders = delta_table(Order)

    def clean(self, order: Order) -> None:
        delta_delete(order, where=order.status == "legacy")


class Report(Transform):
    orders = delta_input(Order)
    selected = output(Order)

    def select(self, order: Order) -> Order:
        return Order.project(order)


pipeline = Cleanup(orders=table_handle).to(Report())
```

The downstream stage reads after the earlier commit. Table outputs retain the caller's `DeltaTable` object even when
stage names or public output names differ. Each mutation commits separately, so a later failure leaves earlier
commits in place. Use `delta_snapshot` when a historical relation is required; ordinary table references track the
live table.

### History, detail, and maintenance

History and detail are normal typed relation results, with the return annotation selecting an ordinary DataFrame output:

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

`limit` is `None` or a positive integer; `variable(int)` lets one compiled transform read a different number of commits
per invocation. History is newest first. Detail and history schemas vary across Delta versions, so declare only the
fields the transform needs. To inspect liquid-clustering keys, expose Delta's `clusteringColumns` field as a typed
string array:

```python
class ClusteredDetail(Schema):
    clustering_columns = array(string(), contains_null=False, alias="clusteringColumns")
```

Restore, optimize, and vacuum are isolated effect steps. A same-schema restore returns the table relation directly; its
annotation must match the bound table schema:

```python
class RestoreOrders(Transform):
    orders = delta_table(Order)
    version = variable(int)

    def restore(self, order: Order) -> Order:
        return delta_restore(order, version=self.version).execute()
```

If the selected version has a different shape, bind the current table as `delta_input(Current)`, declare
`delta_output(Restored)`, and return the restore operation with `-> Restored`. Structure validates the current schema
before restoring and validates the declared restored schema afterward.

#### Compaction and Z-ordering

Compaction combines small files. Z-ordering groups related column values into files to improve data skipping on
filtered reads. Choose either operation explicitly; running a transform executes its maintenance steps every time.

```python
class CompactOrders(Transform):
    orders = delta_table(Order)

    def compact(self, order: Order) -> None:
        delta_optimize(order).execute_compaction()


class ZOrderOrders(Transform):
    orders = delta_table(Order)

    def optimize(self, order: Order) -> None:
        delta_optimize(order).execute_zorder(by=(order.id, order.status))
```

`delta_optimize(target, *, where=None)` requires a `delta_table(Schema)` target. Omit `where=` to optimize the
whole table, or supply a Boolean predicate using its partition columns to select partitions. Runtime variables may
provide predicate values. For example, when `order_date` is a partition column:

```python
delta_optimize(order, where=order.order_date == self.selected_date).execute_zorder(
    by=(order.customer_id, order.product_id),
)
```

`execute_zorder(*, by)` requires a non-empty tuple of distinct top-level fields from that target. Structure resolves
field aliases to physical column names. Computed expressions, nested fields, and fields from other tables are rejected.
A builder may be executed once. Delta enforces its own requirements for Z-order keys and table layout.

Both operations preserve logical rows and schema and return no metric relation. The transform result retains the
caller's original table handle. Z-ordering does not guarantee query-result ordering; use an explicit query sort when
needed. See [choosing columns](../background/DeltaIceberg.back.md#choosing-z-order-columns) and the
[runnable maintenance examples](../recipes/DeltaInspectionAndMaintenance.md#compaction-and-z-ordering).

#### Liquid clustering

Liquid clustering is configured on the native table by its caller. `delta_optimize(order).execute_compaction()`
delegates incremental optimization to Delta. The terminal `.full()` operation forces reclustering of existing data
using the caller-configured active keys and requires those keys to be active. On admitted open-source profiles,
predicates and Z-order are unsupported for tables with the clustering feature, even after keys are cleared. Databricks
`OPTIMIZE FULL WHERE` is not admitted.


```python
class ReclusterOrders(Transform):
    orders = delta_table(Order)

    def optimize(self, order: Order) -> None:
        delta_optimize(order).full()
```

This terminal operation requires active clustering keys and does not accept `where=`.

#### Vacuum retention

Vacuum uses Delta's 168-hour default retention. Short retention requires the explicit `allow_short_retention=True`
argument, and Delta's own safety guard remains enabled:

```python
class VacuumOrders(Transform):
    orders = delta_table(Order)

    def vacuum(self, order: Order) -> None:
        delta_vacuum(order).execute()  # keeps Delta's default 168-hour retention
```

Vacuum permanently removes eligible unreferenced files and can make older time-travel reads unavailable. The caller
must ensure active readers and streams no longer need those files. Structure does not switch off Delta's native safety
check.

## Apache Iceberg tables

Structure binds existing Iceberg format-v2 tables by their Spark catalog identifiers. The caller supplies the Spark
session, Iceberg runtime, catalog, credentials, and tables. See the
[combined compatibility ledger](../compatibility/DeltaIceberg.compat.md#apache-iceberg) for admitted runtime pairs.
The examples use the `Order` and `Change` schemas declared above and the same imports from `structure` and
`structure.plugin.pyspark`.

### Bind an existing table

| Structure API | Purpose | Example |
| --- | --- | --- |
| `iceberg_input(Schema)` | Read-only table or source of an evolving append | `current = iceberg_input(Order)` |
| `iceberg_table(Schema)` | Reads and same-schema mutations | `orders = iceberg_table(Order)` |
| `iceberg_output(Schema)` | Result schema of an evolving append | `orders = iceberg_output(OrderV2)` |

Pass a fully qualified catalog identifier at invocation. Table columns, types, and nullability must match the declared
current schema. A table output returns the supplied identifier, rather than a DataFrame:

```python
class AppendIcebergOrders(Transform):
    changes = input(Order)
    orders = iceberg_table(Order)

    def append(self, change: Order, order: Order) -> None:
        iceberg_append(order, change).execute()


result = AppendIcebergOrders(changes=changes_df, orders="warehouse.sales.orders").run(session)
assert result.orders == "warehouse.sales.orders"
```

Catalog names remain runtime bindings, so the same compiled transform can target different tables. An
`iceberg_output` is supplied by the transition step; do not pass it as an invocation argument or step parameter.

### Same-schema row mutations

`iceberg_update(target, *, where, set)` and `iceberg_delete(target, *, where)` require an `iceberg_table` target and an
explicit Boolean predicate. `where=True` affects every row. Update assignments use a partial target schema value;
their expressions refer to the target's fields and runtime variables.

```python
class CleanIcebergOrders(Transform):
    orders = iceberg_table(Order)

    def clean(self, order: Order) -> None:
        iceberg_update(order, where=order.status == "legacy", set=Order(status="pending"))
        iceberg_delete(order, where=order.status == "cancelled")
```

Use a merge to match incoming rows against the target and assign values from both relations:

```python
class MergeIcebergOrders(Transform):
    changes = input(Change)
    orders = iceberg_table(Order)

    def merge(self, change: Change, order: Order) -> None:
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


result = MergeIcebergOrders(changes=changes_df, orders="warehouse.sales.orders").run(session)
assert result.orders == "warehouse.sales.orders"
```

`iceberg_merge(target, source, *, on)` supports ordered `when_matched_update`, `when_matched_delete`,
`when_matched_update_all`, `when_not_matched_insert`, `when_not_matched_insert_all`,
`when_not_matched_by_source_update`, and `when_not_matched_by_source_delete` clauses. Clauses accept `condition=`;
explicit updates and inserts take `set=` and `values=`. Insert values must supply required target fields.
Finish merge and append builders with `.execute()`; each builder can execute once. Native Iceberg and Spark determine
multiple-match and commit behavior.

### Evolve an append schema

Schema evolution is explicit and local to one append. Declare the current table through `iceberg_input(Current)`,
declare `iceberg_output(New)`, and return the evolving append with `-> New`. The `to=` schema must match that output:

```python
class EvolveIcebergOrders(Transform):
    changes = input(OrderV2)
    current_orders = iceberg_input(Order)
    orders = iceberg_output(OrderV2)

    def append(self, change: OrderV2, order: Order) -> OrderV2:
        return iceberg_append(order, change).with_schema_evolution(to=OrderV2).execute()


result = EvolveIcebergOrders(changes=changes_v2_df, current_orders="warehouse.sales.orders").run(session)
assert result.orders == "warehouse.sales.orders"
```

Here `OrderV2` adds the nullable `note` field defined in the Delta evolution example. Existing columns retain their
types and nullability, and added columns must be nullable. Helper evolution supports additive changes through append;
other schema changes use native SQL.

The caller enables the table property before the append:

```sql
ALTER TABLE warehouse.sales.orders
SET TBLPROPERTIES ('write.spark.accept-any-schema' = 'true')
```

Structure applies the native schema-merge writer option to this append and validates the declared result schema after
the commit. It does not alter table properties or session settings. Bind later invocations with the new schema.

### Snapshot reads

`iceberg_snapshot(target, *, snapshot_id=None, timestamp=None)` reads a retained historical state. Supply exactly one
selector, either an integer snapshot ID or a timezone-aware `datetime`. A selector may be a runtime `variable()`:

```python
class ReadIcebergSnapshot(Transform):
    orders = iceberg_input(Order)
    snapshot_id = variable(int)
    snapshot = output(Order)

    def read(self, order: Order) -> Order:
        return iceberg_snapshot(order, snapshot_id=self.snapshot_id)


snapshot = ReadIcebergSnapshot(
    orders="warehouse.sales.orders", snapshot_id=retained_snapshot_id
).run(session).snapshot
```

Return the helper directly from a single-output step. The return annotation describes the selected historical schema;
the result is an ordinary DataFrame. Iceberg snapshot IDs are opaque native identifiers, with no sequential Delta
version relationship. Expired snapshots cannot be read.

### Execution and failure

Row mutations and maintenance are batch effects against existing tables. Each call commits independently. Structure
does not combine steps into a transaction, retry uncertain commits, or undo earlier commits after a later failure.
The transform result retains the caller's catalog identifier, and later table reads see earlier committed changes.
Streaming query startup, checkpoints, and shutdown remain caller responsibilities.

### Inheritance and composition

Iceberg declarations retain their provider, role, and schema through inheritance. An override replaces an inherited
method at its scheduled position. Calling `super()` schedules the parent's full step before the child's operations:

```python
class CleanupIcebergOrders(Transform):
    orders = iceberg_table(Order)

    def clean(self, order: Order) -> None:
        iceberg_delete(order, where=order.status == "cancelled")


class NormalizeIcebergOrders(CleanupIcebergOrders):
    def clean(self, order: Order) -> None:
        super().clean(order)
        iceberg_update(order, where=order.status == "legacy", set=Order(status="pending"))


class ReportIcebergOrders(Transform):
    orders = iceberg_input(Order)
    selected = output(Order)

    def select(self, order: Order) -> Order:
        return Order.project(order)


pipeline = NormalizeIcebergOrders(orders="warehouse.sales.orders").to(ReportIcebergOrders())
selected_orders = pipeline.run(session).selected
```

Composition passes the same catalog identifier from an `iceberg_table` or explicit evolution result to a matching
`iceberg_input` or `iceberg_table` stage. Provider and schema mismatches fail during compilation. A read-only wrapper
input cannot supply a mutable stage role. Stage names and output aliases do not change the table identifier. Consumers
read after earlier commits; request `iceberg_snapshot` when historical contents are required.

### History, snapshots, and metadata

`iceberg_history(target, *, limit=None)` reads changes to the current snapshot, while
`iceberg_snapshots(target, *, limit=None)` reads snapshot metadata such as operation and commit time. These return
ordinary typed DataFrames. Declare the native fields needed by each result; history and snapshots have different
schemas:

```python
class IcebergHistory(Schema):
    snapshot_id = long()
    made_current_at = timestamp()
    is_current_ancestor = boolean()


class IcebergSnapshotInfo(Schema):
    snapshot_id = long()
    committed_at = timestamp()
    operation = string()


class InspectIcebergCommits(Transform):
    orders = iceberg_input(Order)
    limit = variable(int, default=5)
    history_rows = output(IcebergHistory)
    snapshot_rows = output(IcebergSnapshotInfo)

    @step(input=orders, output=history_rows)
    def history(self, order: Order) -> IcebergHistory:
        return iceberg_history(order, limit=self.limit)

    @step(input=orders, output=snapshot_rows)
    def snapshots(self, order: Order) -> IcebergSnapshotInfo:
        return iceberg_snapshots(order, limit=self.limit)
```

`limit` is `None` or a positive integer, including a runtime variable. History is newest first by `made_current_at`;
snapshots are newest first by `committed_at`. The explicit `@step` bindings make both methods read the original table.

`iceberg_metadata(target, *, kind, to)` selects `history`, `snapshots`, `files`, `manifests`, `partitions`, or `refs`.
The `to=` schema must match the method's return annotation:

```python
class IcebergDataFile(Schema):
    file_path = string()
    record_count = long()


class InspectIcebergFiles(Transform):
    orders = iceberg_input(Order)
    files = output(IcebergDataFile)

    def inspect(self, order: Order) -> IcebergDataFile:
        return iceberg_metadata(order, kind="files", to=IcebergDataFile)
```

### Rollback and file maintenance

Maintenance requires an `iceberg_table` target and a terminal `.execute()`. Helpers retain the caller's identifier
and do not return native procedure metrics as transform outputs.

`iceberg_rollback(target, *, snapshot_id=None, timestamp=None)` takes exactly one selector. It follows native ancestry
rules and requires the selected snapshot to remain available:

```python
class RollbackIcebergOrders(Transform):
    orders = iceberg_table(Order)
    snapshot_id = variable(int)

    def rollback(self, order: Order) -> None:
        iceberg_rollback(order, snapshot_id=self.snapshot_id).execute()
```

`iceberg_rewrite_data_files` accepts `strategy=`, `sort_order=`, a typed target-only `where=` predicate, and an
`options=` mapping of strings to strings. `iceberg_rewrite_manifests` accepts `use_caching=` and a nonnegative
`spec_id=`. Each procedure executes separately:

```python
class RewriteIcebergOrders(Transform):
    orders = iceberg_table(Order)

    def rewrite(self, order: Order) -> None:
        iceberg_rewrite_data_files(order, strategy="binpack").execute()
        iceberg_rewrite_manifests(order, use_caching=True).execute()
```

### Snapshot expiration and orphan cleanup

`iceberg_expire_snapshots(target, *, older_than=None, retain_last=None)` expires eligible snapshots while retaining
native reference safeguards. `retain_last` is a positive integer literal. `older_than` is a timezone-aware `datetime`
literal or runtime variable:

```python
from datetime import datetime


class ExpireIcebergSnapshots(Transform):
    orders = iceberg_table(Order)
    cutoff = variable(datetime)

    def expire(self, order: Order) -> None:
        iceberg_expire_snapshots(order, older_than=self.cutoff, retain_last=5).execute()
```

`iceberg_remove_orphan_files(target, *, older_than=None, dry_run=False)` removes eligible files that are no longer
referenced. The same timestamp rules apply. Use `dry_run=True` to request a run without deleting files:

```python
class CheckIcebergOrphans(Transform):
    orders = iceberg_table(Order)
    cutoff = variable(datetime)

    def check(self, order: Order) -> None:
        iceberg_remove_orphan_files(order, older_than=self.cutoff, dry_run=True).execute()
```

The caller coordinates retention with active readers and writers and scopes cleanup to the intended warehouse.
To inspect dry-run candidates or procedure metrics, use `sql(..., to=ResultSchema)` with the native result schema.

### SQL and provider boundaries

The typed `sql(...)` helper supports Iceberg queries, commands, and procedures. A table binding passed through
`relations` resolves its runtime catalog identifier. Bind predicate values through `args`:

```python
class ReadOpenIcebergOrders(Transform):
    orders = iceberg_input(Order)
    open_orders = output(Order)

    def read(self, order: Order) -> Order:
        return sql(
            "SELECT id, status FROM {order} WHERE status = :status",
            relations={"order": order},
            args={"status": "open"},
            to=Order,
        )


open_orders = ReadOpenIcebergOrders(orders="warehouse.sales.orders").run(session).open_orders
```

DataFrame relations continue to bind row scopes. Catalog administration, table creation, branch/tag management,
arbitrary DDL, and procedures without helpers remain caller-owned SQL. Generic SQL does not imply helper admission
for every provider operation. Iceberg changelog helpers and non-v2 table formats are outside this convenience API.

See the [table background](../background/DeltaIceberg.back.md#apache-iceberg-tables) for rationale and lifecycle
details, and the [Iceberg API](../api/DeltaIceberg.api.md#apache-iceberg) for the operation inventory.
