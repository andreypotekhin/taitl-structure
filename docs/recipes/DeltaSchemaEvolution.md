# Evolve a Delta Table Schema Explicitly

Schema evolution is opt-in and the method return annotation declares the expected post-commit schema. Keep the current
table as a read-only `delta_input` and declare the evolved result with `delta_output`.

```python
from structure import Schema, Transform, input
from structure.plugin.pyspark import delta_input, delta_merge, delta_output, integer, string


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
            .with_schema_evolution()
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
```

Compilation checks that the result retains compatible existing fields and that source/assignments can provide new
fields. At runtime, Structure checks the old table shape before the merge and validates the declared `OrderV2` shape
after the commit. Merge evolution uses Delta's `withSchemaEvolution()`; append evolution scopes `mergeSchema=true` to
one writer. Neither operation enables session-wide auto-merge.
