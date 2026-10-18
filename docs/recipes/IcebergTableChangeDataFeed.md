# Process an Iceberg Table Changelog in a Transform

**Problem:** A consumer needs typed row changes between two retained Iceberg snapshots.

**Solution:** Create a changelog view with Iceberg's native Spark procedure, then bind its DataFrame to an ordinary
Structure input.

## Create the native changelog view

The caller provides an Iceberg-capable Spark session, a format-v2 table `warehouse.sales.orders` with string columns
`id` and `status`, and two retained snapshot IDs from the same table history. This workflow uses native SQL;
Structure's Iceberg convenience API does not include a changelog helper.

Set `start_snapshot_id` and `end_snapshot_id` to the selected integer IDs. Use a distinct temporary view name for
concurrent jobs in the same Spark session:

```python
start_id = int(start_snapshot_id)
end_id = int(end_snapshot_id)
spark.sql(f"""
    CALL warehouse.system.create_changelog_view(
        table => 'sales.orders',
        changelog_view => 'order_changes',
        options => map(
            'start-snapshot-id', '{start_id}',
            'end-snapshot-id', '{end_id}'
        ),
        compute_updates => true,
        identifier_columns => array('id')
    )
""")
changes_df = spark.table("order_changes")
```

The start snapshot is exclusive and the end snapshot is inclusive. With `compute_updates=true`, the procedure pairs
changes using `id` and exposes `INSERT`, `DELETE`, `UPDATE_BEFORE`, and `UPDATE_AFTER` change types. Choose identifier
columns that identify an order consistently. See
[Iceberg's changelog procedure](https://iceberg.apache.org/docs/latest/spark-procedures/#create_changelog_view)
for native options and update-pairing requirements.

## Declare row and change metadata

The changelog contains the table fields plus native metadata. Aliases give those columns convenient Python names:

```python
from structure import *
from structure.plugin.pyspark import *


class OrderChange(Schema):
    id = string()
    status = string()
    change_type = string(alias="_change_type")
    change_ordinal = integer(alias="_change_ordinal")
    commit_snapshot_id = long(alias="_commit_snapshot_id")


class SelectOrderUpdates(Transform):
    changes = input(OrderChange)
    updates = output(OrderChange)

    def select(self, change: OrderChange) -> OrderChange:
        where(change.change_type == "UPDATE_AFTER")
        return change
```

This example keeps update postimages. Include `INSERT` and `DELETE` in the filter when the consumer needs those
events too; preserving metadata lets the consumer distinguish their effects.

## Run and advance the consumer position

Run the typed transformation with the caller-created batch DataFrame:

```python
session = StructureSession(spark=spark)
updates = SelectOrderUpdates(changes=changes_df).run(session).updates
```

After the consumer has persisted or processed the selected events, it can save `end_id` as the next batch's exclusive
start. The caller manages that position and sink idempotence. Keep snapshots and files needed for replay available
until processing finishes. Drop the temporary view with `spark.catalog.dropTempView("order_changes")` after the
consumer finishes using its DataFrame.

An append-only incremental reader does not provide the same row-change semantics. This example is a native batch
changelog workflow; it does not start a streaming query or extend Structure's admitted helper surface. The caller
must validate procedure availability and behavior on its Spark/Iceberg runtime, including the server for Connect.

See the [Iceberg reference](../reference/DeltaIceberg.ref.md#sql-and-provider-boundaries) for the SQL interface and
the [compatibility ledger](../compatibility/DeltaIceberg.compat.md#apache-iceberg) for the tested helper scope.
