# Schema Declaration Syntax

Structure schemas declare row contracts for compiler checks, generated Spark `StructType` code, runtime validation,
traceability, and IDE navigation. Field factories make both a field's type and its options visible in one declaration.

For the concise API inventory, see the [Schemas API](../api/Schemas.api.md). The public schema contract is defined in
the [Schema reference](../reference/Schema.ref.md).

## Canonical Form

```python
from structure import Schema
from structure.field import *


class OrderRaw(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = string(nullable=True)


class OrderNormalized(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = decimal(12, 2, nullable=True)
```

Each assignment has two parts: the Python attribute name and a field factory. The attribute name is used in Structure
source; `alias=` supplies a different physical Spark column name when necessary.

## Imports And Grammar

Use field factories from `structure.field` for schema declarations:

```python
from structure import Schema
from structure.field import *
```

Use `structure.types` only when an API needs a standalone type object, such as a cast or UDF return contract.

```text
schema_class      := class NAME(Schema): field_decl*
field_decl        := NAME = field_factory(field_kwarg*)
field_factory     := scalar_type | decimal_type | array_type | struct_type | map_type
scalar_type       := string() | integer() | long() | float() | double() | boolean() | date() | timestamp()
decimal_type      := decimal(precision, scale)
array_type        := array(field_factory, contains_null=BOOL?)
struct_type       := struct(schema_ref)
map_type          := map(field_factory, field_factory, value_contains_null=BOOL?)
field_kwarg       := nullable=BOOL | alias=STRING | metadata=DICT | description=STRING
schema_ref        := Schema class object
```

## Field Options

Every field factory accepts these common options:

```python
class CustomerSource(Schema):
    customer_id = string(
        nullable=False,
        alias="customer-id",
        metadata={"source": "crm", "pii": "indirect"},
        description="Stable identifier supplied by the CRM.",
    )
```

- `nullable` defaults to `True`.
- `alias` is a non-empty physical Spark column name.
- `metadata` is an immutable mapping retained for generated documentation, diagnostics, and traceability.
- `description` is a user-facing field description.

Field declaration order is schema, projection, validation, and generated-documentation order. Field factories do not
declare keys or uniqueness; express those business rules in the pipeline or a dedicated data-quality contract.

## Nested Types

Use nested field factories inside `array(...)` and `map(...)`. Their nested declarations describe only the type;
`contains_null=` and `value_contains_null=` control nested nullability.

```python
class Address(Schema):
    city = string(nullable=True)
    postal_code = string(nullable=True)


class Order(Schema):
    tags = array(string(), contains_null=False, nullable=True)
    attributes = map(string(), string(), value_contains_null=False, nullable=True)
    shipping = struct(Address, nullable=True)
```

`struct(Address)` identifies that schema's effective inherited shape. Recursive struct declarations are rejected.
Spark map keys cannot be null, and nested maps are not valid map keys.

## Output Construction

Inside a compiled step method, call a schema class to declare a symbolic projection:

```python
return OrderNormalized(
    id=order.id,
    customer_id=lower(trim(order.customer_id)),
    total=to_decimal(order.total, precision=12, scale=2),
)
```

Nested structs use the nested schema constructor:

```python
return OrderPublished(
    id=order.id,
    shipping=Address(
        city=trim(order.shipping.city),
        postal_code=order.shipping.postal_code,
    ),
)
```

All output fields must be supplied or copied with `Schema.base(...)`; positional arguments, unknown fields, and missing
fields fail during compilation.

## Retired Syntax

`field(...)` wrappers and capitalized declaration type constructors are no longer supported:

```text
field(String(), nullable=False)        -> string(nullable=False)
field(Decimal(12, 2), nullable=True)   -> decimal(12, 2, nullable=True)
field(Array(String()))                  -> array(string())
field(Struct(Address))                  -> struct(Address)
```

Field factories reject unsupported options immediately. In particular, `primary_key=` is not a schema declaration
option; it does not establish join cardinality or runtime uniqueness validation.
