# Colocated Intermediate Schemas

Keep reusable row contracts in model schema files. When a shape genuinely belongs
to one transform only, Structure also accepts a module-level intermediate schema
beside that transform.

This is a source-organization choice, not a private runtime type: Structure still
validates the intermediate DataFrame and emits its Spark schema, generated
documentation, and traceability entry.

## Scenario

An order normalization is useful only as the first stage of one publishing
transform. The raw input and published output remain shared model contracts;
only the normalization shape is colocated.

```python
# src/orders/transforms/publish.py
from structure import Schema, Transform, input, output
from structure.plugin.pyspark import field
from orders.schemas.order import OrderPublished, OrderRaw


class OrderNormalized(Schema):
    id = field.string(nullable=False)


class PublishOrders(Transform):
    orders = input(OrderRaw)
    published = output(OrderPublished)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        return OrderNormalized(id=order.id)

    def publish(self, order: OrderNormalized) -> OrderPublished:
        return OrderPublished(id=order.id)
```

`OrderNormalized` is a normal schema contract. `structure check` discovers it,
and `structure compile` creates a generated schema module alongside the generated
transform module. Use a dedicated model schema file instead when another
transform, read, write, or caller needs the same shape.

Do not nest the schema in `PublishOrders`. Declare it at module scope as shown;
Structure reports an actionable compile error for a `Schema` class nested inside a
transform.

See the [schema API](../api/Schemas.api.md) and [source module rules](../background/SourceModuleRules.back.md) for
the broader declaration and discovery contracts.
