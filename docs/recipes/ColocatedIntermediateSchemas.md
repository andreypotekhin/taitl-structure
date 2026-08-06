# Colocated Intermediate Schemas

**Problem:** An order-normalization shape belongs only to one publishing transform,
but placing it in the shared model package makes it look reusable and nesting it
inside `PublishOrders` prevents Structure from discovering it.

**Solution:** Declare the intermediate schema at module scope beside the transform
so the raw input and published output remain shared contracts while Structure
validates the intermediate DataFrame and emits its schema, generated documentation,
and traceability entry.

## Declare and use the colocated schema

Place the intermediate `OrderNormalized` contract at module scope, then pass it
between the transform's steps.

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

`OrderNormalized` is a normal schema contract, not a private implementation type.
The `normalize` step produces it, and `publish` consumes it as its input.

## Keep the contract discoverable

`structure check` discovers the module-level schema, and `structure compile`
creates a generated schema module alongside the generated transform module. Use a
dedicated model schema file instead when another transform, read, write, or caller
needs the same shape.

Do not nest `OrderNormalized` in `PublishOrders`. Structure reports an actionable
compile error for a `Schema` class nested inside a transform.

See the [schema API](../api/Schemas.api.md) and [source module rules](../background/Generation.back.md) for
the broader declaration and discovery contracts.
