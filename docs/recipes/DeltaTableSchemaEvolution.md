# Evolve a Delta Table Schema Explicitly

**Problem:** Incoming order changes add an optional note, while the existing table still has the original schema.

**Solution:** Declare the current table with `delta_input`, the evolved result with `delta_output`, and return a merge
with explicit schema evolution.

## Declare the current and result schemas

The caller supplies a Delta-capable Spark session, a native `DeltaTable` matching `Order`, and a source DataFrame
matching `ChangeV2`. `OrderV2` preserves the existing fields and adds a nullable `note`:

```python
from structure import *
from structure.plugin.pyspark import *


class Order(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)


class ChangeV2(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)
    note = string()


class OrderV2(Order):
    note = string()


class EvolveOrders(Transform):
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
```

The `to=` schema must match the method's return annotation and declared output. Do not supply the evolved output as an
invocation argument or step parameter. Compilation checks that existing fields remain compatible and that the source
can provide the new fields.

## Merge and use the evolved table

Structure validates the current schema before merging and the declared result schema after the native commit:

```python
session = StructureSession(spark=spark)
result = EvolveOrders(changes=changes_v2_df, current_orders=table).run(session)
assert result.orders is table
```

Here `table` is the caller's native `DeltaTable`. Later invocations bind it using `OrderV2`. Composition can pass the
evolved result to a consumer that declares that schema:

```python
class ReadEvolvedOrders(Transform):
    orders = delta_input(OrderV2)
    selected = output(OrderV2)

    def read(self, order: OrderV2) -> OrderV2:
        return OrderV2.project(order)


pipeline = EvolveOrders(changes=changes_v2_df, current_orders=table).to(ReadEvolvedOrders())
selected_orders = pipeline.run(session).selected
```

Choose either the direct invocation or the pipeline for a given batch. Each execution performs the merge again;
Structure does not track whether the caller has already processed that batch.

## Choose append evolution when matching is unnecessary

An evolving append uses the same current/result declarations and adds all incoming rows:

```python
class AppendEvolvedOrders(Transform):
    changes = input(ChangeV2)
    current_orders = delta_input(Order)
    orders = delta_output(OrderV2)

    def append(self, change: ChangeV2, order: Order) -> OrderV2:
        return delta_append(order, change).with_schema_evolution(to=OrderV2).execute()
```

Merge evolution uses Delta's `withSchemaEvolution()`. Append evolution scopes `mergeSchema=true` to one writer.
Neither enables session-wide auto-merge. A same-schema write cannot silently add fields. A failure during result
validation can occur after the native commit; Structure does not undo that commit.

See the [schema-evolution reference](../reference/DeltaIceberg.ref.md#explicit-schema-evolution) and
[table background](../background/DeltaIceberg.back.md#a-deliberate-schema-transition) for compatibility and lifecycle
rules.
