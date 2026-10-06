# Delta Tables API

Structure can compile typed mutations against an existing Delta table. The caller creates the table, provisions its
native constraints, and passes a `delta.tables.DeltaTable` object to the transform. Delta support is **implemented;
release-gated**: ordinary PySpark 4.1.0 with Delta 4.1.0 has isolated live evidence, while the wider V11 admission
matrix is pending. See [Delta compatibility](../compatibility/DeltaTables.compat.md) before adopting it.

Import `Schema`, `Transform`, `input`, `transform`, and `StructureSession` from `structure`. Import the Delta
declarations, operations, `check`, and field factories from `structure.plugin.pyspark`.

## Declarations

| Structure API | Purpose | Example |
| --- | --- | --- |
| `delta_input(Schema)` | Caller-bound, read-only Delta relation | `current_orders = delta_input(OrderV1)` |
| `delta_table(Schema)` | Caller-bound relation that may be read and mutated in place | `orders = delta_table(Order)` |
| `delta_output(Schema)` | Declared result schema for an explicit schema transition | `orders = delta_output(OrderV2)` |
| `check(predicate, name=None)` | Expected native Delta CHECK | `check(status != "invalid", name="valid_status")` |

Declare CHECKs in `Schema.constraints`. Structure checks that the bound table has matching native CHECK metadata; it
does not create the table or install constraints. Provision the corresponding native constraint when creating the
table, or add it explicitly:

```sql
ALTER TABLE delta.`/path/to/orders`
ADD CONSTRAINT valid_status CHECK (status <> 'invalid')
```

Table columns, types, and nullability must match the current declared schema before mutation. For explicit evolution,
the new output schema is checked after the commit. Every Delta binding needs a native `DeltaTable`, not a DataFrame.

## Same-schema mutations

The target is a relation parameter in an effect step. A plain typed method can infer its same-schema mutation target and
return `None`; `@step` can disambiguate relations when a schema is used more than once. Authors who prefer a typed return
may return a merge operation directly and annotate the method with the target schema. Structure checks that this
annotation matches the bound table.

```python
from structure import Schema, StructureSession, Transform, input, step, transform
from structure.plugin.pyspark import (
    check, delta_delete, delta_merge, delta_table, delta_update, string,
)


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)
    constraints = (check(status != "invalid", name="valid_status"),)


class Change(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


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


# The caller has already created the table with the declared CHECK.
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
must supply every non-nullable target field. Predicates and assignments use typed Structure expressions, not SQL strings.

`delta_merge(target, source, on=...)` requires a target/source match expression. Its ordered builder supports
`when_matched_update`, `when_matched_delete`, `when_matched_update_all`, `when_not_matched_insert`,
`when_not_matched_insert_all`, `when_not_matched_by_source_update`, and `when_not_matched_by_source_delete`.
Clause methods accept `condition=` where Delta permits it; update and insert methods accept `set=` and `values=`
respectively. Finish the builder with `.execute()`. `delta_append(target, source).execute()` appends a source relation.

## Explicit schema evolution

For an expected schema change, declare the current table as `delta_input(CurrentSchema)` and the result as
`delta_output(NewSchema)`. The step's return annotation resolves that output. Return exactly one merge or append
operation with `.with_schema_evolution()`; do not pass the new output as a step parameter or invocation argument.

```python
from structure.plugin.pyspark import delta_input, delta_merge


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
            .with_schema_evolution()
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )


result = EvolvingMerge(changes=changes_v2_df, current_orders=table).run(StructureSession(spark=spark))
assert result.orders is table
```

Merge evolution maps to Delta's `withSchemaEvolution()`. The append form is
`return delta_append(order, change).with_schema_evolution().execute()`; it applies `mergeSchema=true` to that append
writer. Neither form changes a session-wide setting. Structure checks the old shape before the commit and the declared
new shape and CHECK metadata afterward. For a later invocation, bind the table using its new schema.

## Snapshot and change-feed reads

Use `delta_snapshot` or `delta_changes` as the direct return value of a single-output step. The result annotation
declares the output Schema. CDF metadata fields use their native Delta column names as aliases:

```python
from structure import Schema, Transform, output, transform, variable
from structure.plugin.pyspark import delta_changes, delta_input, long, string, timestamp


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

`variable(type, default=...)` supplies a runtime scalar without specializing the compiled transform for each value.
It can appear in Spark expressions and Delta selectors. It cannot control Python `if` statements or graph shape; use
`parameter()` for compile-time choices. Supported scalar types are `bool`, `int`, `float`, `str`, `bytes`, `Decimal`,
`date`, and timezone-aware `datetime`; `Decimal` declarations require `precision=` and `scale=`. A Delta snapshot
accepts exactly one of `version=` or `timestamp=`. `delta_changes` accepts `starting_version=` with optional
`ending_version=`, or `starting_timestamp=` with optional `ending_timestamp=`. The range endpoints are inclusive.

The caller enables CDF on the table before writing changes:

```sql
ALTER TABLE delta.`/path/to/orders`
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

The Spark session must include `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` and
`spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`. Structure checks the table property
and session configuration before a CDF read. CDF only includes changes committed after the property was enabled, and
the requested versions or timestamps must still be available in Delta history.

`delta_replace_where(target, source, where=...).execute()` performs a same-schema selective overwrite. The predicate
may reference target fields and runtime variables. The source must use the same Structure Schema; Delta also checks
that incoming rows satisfy the predicate. The call is a native commit and is retained as a step effect.

## Streaming CDF boundary

Structure does not start or manage a streaming CDF reader. Bind the caller-created stream as an ordinary streaming
input, transform it through a normal typed step, then let the caller own `writeStream` and its checkpoint:

```python
from structure import Schema, Transform, input, output, transform
from structure.plugin.pyspark import where


@transform(streaming=True)
class SelectOrderChanges(Transform):
    changes = input(OrderChange, streaming=True)
    updates = output(OrderChange)

    def select(self, change: OrderChange) -> OrderChange:
        where(change.change_type == "update_postimage")
        return OrderChange.project(change)


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

## CHECK comparison and runtime behavior

`delta_check_match` may be set in PySpark plugin configuration, `@transform(...)`, or `@step(...)`; the nearest setting
wins. The default, `"expression"`, compares native and declared CHECK predicates after normalizing supported SQL
syntax. `"name"` checks names without comparing predicates. `"off"` skips CHECK verification. Table shape checks
remain active in every mode. An unsupported native CHECK expression fails under the default mode; use `"name"` only
when name matching is sufficient for your table policy.

Delta operations are batch-only. Each operation makes its own native commit; Structure does not combine steps into a
transaction or retry uncertain commits. A later failure does not undo an earlier success. Online and generated modes
run the same checked operations. The returned `DeltaTable` is the caller's original object; if its `toDF()` was
materialized earlier, reopen the table to inspect the latest snapshot.

For rationale and lifecycle details, see [Delta table background](../background/DeltaTables.back.md). For recipes, see
[same-schema mutations](../recipes/DeltaTableMutations.md), [schema evolution](../recipes/DeltaSchemaEvolution.md), and
[change data feed](../recipes/DeltaChangeDataFeed.md). For the exact
release and target limits, see [Delta compatibility](../compatibility/DeltaTables.compat.md).
