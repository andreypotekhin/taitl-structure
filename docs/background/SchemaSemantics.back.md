# Schema Semantics

Schemas are Structure's typed row contracts. The compiler extracts them without Spark, validates transform projections
against them, and uses the same schema model for online execution, generated PySpark, runtime validation,
traceability, and generated documentation.

The syntax is defined in [Schema Declaration Syntax](SchemaDeclarationSyntax.back.md); the stable public contract is
the [Schema reference](../reference/Schema.ref.md).

## Layers

1. A `Schema` subclass declares PySpark fields with `structure.platform.pyspark.dsl.field` factories.
2. Compilation resolves inheritance into an ordered, backend-neutral schema model.
3. Execution and generated code materialize equivalent Spark `StructType` shapes.
4. Explicit data-quality rules, when configured, add value constraints without changing the base row shape.

```python
from structure import Schema
from structure.platform.pyspark.dsl.field import *


class OrderRaw(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = string(nullable=True)
```

Field names, aliases, types, nullability, metadata, descriptions, and declaration order are part of the contract.
Aliases are physical Spark names; Python attribute names remain the names used in Structure source.

## Contract Rules

- Field factories reject unknown options and invalid type parameters before Spark execution.
- Schema inheritance composes fields in deterministic class-body order.
- Output construction checks field presence, type compatibility, and nullability before generated or online execution.
- Generated code and online execution use the same checked schema model, so neither path may invent a different shape.
- Schema declarations do not imply primary keys, uniqueness, or runtime data-quality enforcement.

## Nested Values

`array(...)`, `map(...)`, and `struct(...)` make nested type and nullability contracts explicit. A nested struct refers
to a particular `Schema` class; recursive struct cycles and invalid map keys are rejected during schema validation.

```python
class Address(Schema):
    city = string(nullable=True)


class Order(Schema):
    shipping = struct(Address, nullable=True)
    tags = array(string(), contains_null=False, nullable=True)
```

See [Nullability and Type Coercion](NullabilityAndTypeCoercion.back.md) for assignment rules and the
[Schemas API](../api/Schemas.api.md) for the available factories.
