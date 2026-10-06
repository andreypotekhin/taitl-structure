# Read Delta Change Data Feed in a Transform

Enable CDF before the writes whose changes should be captured, configure the Delta Spark session, then bind the table
to a typed transform. The CDF output schema aliases Structure fields to Delta's metadata column names.

```sql
ALTER TABLE delta.`/path/to/orders`
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

```python
from structure import Schema, Transform, output, variable
from structure.plugin.pyspark import delta_changes, delta_input, integer, long, string, timestamp


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


events = ReadOrderChanges(
    orders=table,
    starting_version=18,
    ending_version=24,
).run(session).changes
```

The session must include `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` and
`spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`. `variable()` allows version bounds
to vary between invocations without compiling a different transform. CDF versions are inclusive and depend on the
table's history retention. For streaming ingestion, the caller creates the `readStream` CDF DataFrame and owns query
startup, checkpointing, sink, and shutdown; Structure processes the typed streaming input.
