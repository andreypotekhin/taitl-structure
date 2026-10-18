# Read Delta Change Data Feed in a Transform

**Problem:** A consumer needs typed order changes over a selected range of Delta table versions.

**Solution:** Enable CDF on the existing table and return `delta_changes` with runtime version selectors and a schema
that includes Delta's change metadata.

## Enable CDF before the relevant writes

The caller supplies a Delta-capable Spark session and a table matching `Order` below. Enable CDF before committing the
changes that the consumer needs:

```sql
ALTER TABLE delta.`/path/to/orders`
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

The session must have working Delta extension and catalog configuration, including
`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` and
`spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`. Structure checks the CDF table
property and Delta session settings before batch reads by default. See the
[CDF reference](../reference/DeltaIceberg.ref.md#change-data-feed) for preflight configuration.

## Declare the rows and change metadata

Aliases map Python attribute names to Delta's native CDF columns. Declare the row fields the consumer needs and the
metadata needed to identify each event:

```python
from structure import *
from structure.plugin.pyspark import *


class Order(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)


class OrderChange(Schema):
    id = integer(nullable=False)
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
```

`variable()` lets selectors vary between invocations without compiling a different transform. Timestamp selectors are
also available; use `starting_timestamp=` and optional `ending_timestamp=` with timezone-aware values instead of
version selectors.

## Read a batch and advance the consumer position

Here `table` is the caller's native `DeltaTable` handle. Both range endpoints are inclusive:

```python
session = StructureSession(spark=spark)
events = ReadOrderChanges(
    orders=table,
    starting_version=18,
    ending_version=24,
).run(session).changes
```

After the consumer has persisted or processed these events, save version `24` and start the next range at `25`.
Omitting `ending_version` reads through the version available when Delta opens the batch; use an explicit end when
the consumer needs a known checkpoint position. The caller manages that position and sink idempotence, and retains
history and data files needed for replay.

## Select update postimages

CDF includes inserts, deletes, and update preimages/postimages. This transform keeps the postimages while retaining
commit metadata:

```python
class SelectOrderUpdates(Transform):
    changes = input(OrderChange)
    updates = output(OrderChange)

    def select(self, change: OrderChange) -> OrderChange:
        where(change.change_type == "update_postimage")
        return change


pipeline = ReadOrderChanges(
    orders=table, starting_version=18, ending_version=24
).to(SelectOrderUpdates())
updates = pipeline.run(session).updates
```

Include other change types when the consumer needs to apply inserts or deletions too. An update postimage alone does
not describe every change to the table.

## Transform a caller-owned stream

For streaming ingestion, create a Delta CDF DataFrame and bind it as an ordinary streaming input:

```python
@transform(streaming=True)
class SelectStreamingOrderUpdates(Transform):
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
updates = SelectStreamingOrderUpdates(changes=stream).run(session).updates
query = updates.writeStream.format("parquet").option(
    "checkpointLocation", checkpoint_path
).start(updates_path)
```

The caller chooses the sink and owns query startup, checkpointing, and shutdown. The table property and session
prerequisites still apply; Structure's batch CDF preflight does not configure the caller-created stream.

See the [CDF reference](../reference/DeltaIceberg.ref.md#change-data-feed) and
[compatibility ledger](../compatibility/DeltaIceberg.compat.md) for selector rules and admitted runtime profiles.
