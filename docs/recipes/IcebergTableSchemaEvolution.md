# Evolve an Iceberg Table Schema Explicitly

**Problem:** Incoming orders add an optional note, while the existing table still has the original schema.

**Solution:** Declare the current table with `iceberg_input`, the evolved result with `iceberg_output`, and return one
append with explicit schema evolution.

## Enable evolution on the existing table

The caller configures an Iceberg-capable Spark session and provisions a format-v2 table matching `Order` below.
Enable the native table property before appending:

```sql
ALTER TABLE warehouse.sales.orders
SET TBLPROPERTIES ('write.spark.accept-any-schema' = 'true')
```

Structure applies the schema-merge writer option to the selected append. It does not change this property or enable
session-wide schema evolution.

## Declare the current and result schemas

`OrderV2` preserves the existing fields and adds a nullable `note`. The source DataFrame must match `OrderV2`:

```python
from structure import *
from structure.plugin.pyspark import *


class Order(Schema):
    id = string(nullable=False)
    status = string(nullable=False)


class OrderV2(Order):
    note = string()


class EvolveOrders(Transform):
    changes = input(OrderV2)
    current_orders = iceberg_input(Order)
    orders = iceberg_output(OrderV2)

    def append(self, change: OrderV2, order: Order) -> OrderV2:
        return iceberg_append(order, change).with_schema_evolution(to=OrderV2).execute()
```

The `to=` schema and return annotation select the declared evolved output. Do not supply that output as an invocation
argument or step parameter.

## Append and use the evolved table

Structure validates the current schema before appending and the declared result schema after the native commit:

```python
session = StructureSession(spark=spark)
result = EvolveOrders(
    changes=changes_v2_df,
    current_orders="warehouse.sales.orders",
).run(session)
assert result.orders == "warehouse.sales.orders"
```

Later transforms bind this table using `OrderV2`. Composition can pass the evolved result to a consumer that declares
that schema:

```python
class ReadEvolvedOrders(Transform):
    orders = iceberg_input(OrderV2)
    selected = output(OrderV2)

    def read(self, order: OrderV2) -> OrderV2:
        return OrderV2.project(order)


pipeline = EvolveOrders(
    changes=changes_v2_df,
    current_orders="warehouse.sales.orders",
).to(ReadEvolvedOrders())
selected_orders = pipeline.run(session).selected
```

Choose either the direct invocation or the pipeline for a given batch; running both appends that batch twice.
Existing rows receive null for the newly added note. Evolution through this helper supports additive nullable
columns and preserves existing types and nullability. Rename, drop, and other schema changes use native SQL.
An append adds rows; it does not update existing orders by `id`.

See the [Iceberg reference](../reference/DeltaIceberg.ref.md#evolve-an-append-schema) and
[table background](../background/DeltaIceberg.back.md#evolving-append) for schema-transition rules.
