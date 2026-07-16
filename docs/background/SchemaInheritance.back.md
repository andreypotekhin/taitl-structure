# Schema Inheritance

Schema inheritance composes shared fields into one ordered row contract. It is declarative schema composition, not
arbitrary Python mixin behavior.

```python
from structure import Schema
from structure.field import *


class EntityKeys(Schema):
    id = string(nullable=False)
    tenant_id = string(nullable=False)


class AuditFields(Schema):
    created_at = timestamp(nullable=False)
    updated_at = timestamp(nullable=True)


class Order(EntityKeys, AuditFields):
    customer_id = string(nullable=False)
    total = decimal(12, 2, nullable=True)
```

The effective order is `id`, `tenant_id`, `created_at`, `updated_at`, `customer_id`, then `total`. That is the order
used for generated schemas, projection, runtime validation, and documentation.

## Rules

- Direct schema bases are collected left to right; shared diamond ancestors contribute once.
- Local declarations follow inherited fields in class-body order.
- A local field with an inherited name replaces that field in its existing position.
- A subclass must resolve a duplicate field name supplied by unrelated bases with an explicit local declaration.
- All non-`object` bases must be `Schema` classes. Plain Python mixins are rejected.
- Field removal, partial overrides, metadata merging, and local reordering are unsupported.

```python
class SoftDeleteFields(Schema):
    deleted_at = timestamp(nullable=True)


class RequiredDeleteMarker(SoftDeleteFields):
    deleted_at = timestamp(nullable=False)
```

The override replaces the complete field contract: type, nullability, alias, metadata, and description. Schema
inheritance does not declare keys or uniqueness; model those requirements separately.

Nested schemas use the same effective inherited shape:

```python
class AddressBase(Schema):
    city = string(nullable=True)


class ShippingAddress(AddressBase):
    postal_code = string(nullable=True)


class OrderWithShipping(Schema):
    shipping = struct(ShippingAddress, nullable=True)
```

See the [Schema reference](../reference/Schema.ref.md) for aliases, nested types, and output construction.
