# Schema Reference

Schemas are Structure's typed row contracts. Use this page when you need to declare a schema, choose a field type,
construct a schema-shaped output, validate a DataFrame, or correct a schema diagnostic.

The [Schema background](../background/Schema.back.md) explains why the model has these rules, how declarations become
runtime shapes, and where schema behavior stops. This page is the practical operation inventory.

## Declare a Schema

Import `Schema` from `structure` and the PySpark field factories from `structure.plugin.pyspark`:

```python
from structure import *
from structure.plugin.pyspark import *


class OrderRaw(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = string(nullable=True)


class OrderNormalized(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = decimal(12, 2, nullable=True)
```

A schema class inherits from `Schema` and declares fields with factories, Python hints, or both. Schema constructors
are used in transform methods to describe output projections; they do not create Python row objects.

The declaration syntax is ordinary Python class syntax. A field may use a factory, a hint, or a hint with a factory:

```python
from datetime import datetime
from decimal import Decimal


class Order(Schema):
    id: str
    item_count: int
    total: Decimal = decimal(12, 2, nullable=False)
    tags: list[str] = array(string(), contains_null=False)
    observed_at: datetime
```

Bare hints infer default types. A factory supplies details that hints cannot express, such as nullability, aliases,
decimal precision and scale, and collection-member nullability. Hints do not control nullability.

### Declaration rules

- Field names are Python class attribute names.
- Field order is class-body order after supported inheritance is resolved.
- Public schema classes should be import-safe.
- All non-`object` bases must be schema classes.
- Python must be able to construct a valid C3 method-resolution order.
- An annotation without an assigned value declares a field.
- An annotated ordinary assignment remains a class attribute, not a field default.
- Non-schema mixins, raw PySpark fields, implicit Spark type strings, and dataclass-style defaults are not schema
  syntax.
- Unknown field options are declaration errors.

### Python hints

| Hint | Inferred field type |
| --- | --- |
| `str` | `string()` |
| `bool` | `boolean()` |
| `int` | `integer()` |
| `float` | `double()` |
| `datetime.date` | `date()` |
| `datetime.datetime` | `timestamp()` |
| `list[T]` | `array(T)` |
| `dict[K, V]` | `map(K, V)` |
| `Schema` subclass | `struct(SchemaSubclass)` |

`Decimal` requires an explicit `decimal(precision, scale)` factory. Optional and union hints, unparameterized
collections, unsupported hints, and incompatible hint/factory pairs are rejected.

## Fields and Options

Every field factory accepts these common options:

```python
string(
    nullable=True,
    alias=None,
    metadata=None,
    description=None,
)
```

`nullable` defaults to `True`. `alias` is the physical Spark column name; without it, the Python field name is also
the Spark name. `metadata` is an immutable mapping. `description` is carried into generated documentation,
diagnostics, and traceability.

```python
class CustomerSource(Schema):
    customer_id = string(
        nullable=False,
        alias="customer-id",
        metadata={"source": "crm", "pii": "indirect"},
        description="Stable customer identifier supplied by the CRM.",
    )
    display_name = string(nullable=True, description="Name displayed to account users.")
```

`customer_id` is the Python name used in Structure expressions; `customer-id` is the Spark column name. Structure
passes aliases through unchanged. It does not sanitize, normalize, quote, or reinterpret backend identifiers.

Aliases are local to a schema unless the field definition is inherited:

```python
class RawPromotion(Schema):
    promotion_code = string(nullable=True, alias="promo-code")


class NormalizedPromotion(Schema):
    promotion_code = string(nullable=True)


class StillRawPromotion(RawPromotion):
    pass
```

The effective Spark names are `promo-code`, `promotion_code`, and `promo-code`, respectively. Empty aliases,
non-string aliases, duplicate Python field names, and duplicate effective Spark names are rejected.

Schema field `alias=` is different from transform output `.alias(...)` and generated Spark projection `.alias(...)`:

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized).alias("orders")
```

The transform alias names an output. It does not rename a schema field or Spark column.

## Types

Field factories return immutable Structure type values. Type equality is structural:

```text
string() == string()
decimal(12, 2) == decimal(12, 2)
array(string()) == array(string())
struct(Address) == struct(Address)
```

### Scalar types

| Factory | Generated PySpark type |
| --- | --- |
| `string()` | `T.StringType()` |
| `integer()` | `T.IntegerType()` |
| `long()` | `T.LongType()` |
| `float()` | `T.FloatType()` |
| `double()` | `T.DoubleType()` |
| `boolean()` | `T.BooleanType()` |
| `date()` | `T.DateType()` |
| `timestamp()` | `T.TimestampType()` |
| `binary()` | `T.BinaryType()` |

The choice is part of the contract. Structure does not infer a field type from live data.

### Decimal

`decimal(precision, scale)` requires an integer precision of at least one and a scale from zero through the precision:

```python
class Payment(Schema):
    total = decimal(12, 2, nullable=False)
```

`decimal(12, 2)` generates `T.DecimalType(12, 2)`. Missing, non-integer, negative, or out-of-range precision and scale
values are rejected.

### Arrays

`array(item_type, contains_null=True)` declares arrays. The item type must be a Structure type object. Nested arrays
and arrays of structs are supported.

```python
class TaggedOrder(Schema):
    tags = array(string(), contains_null=False, nullable=True)
```

The array may be null, while elements of a present array may not be null.

### Structs

`struct(schema)` declares a nested schema. The argument must be a `Schema` class, not an instance. Nested fields use
the referenced schema's effective field order and identity.

```python
class Address(Schema):
    city = string(nullable=True)
    postal_code = string(nullable=True)


class OrderWithAddress(Schema):
    shipping = struct(Address, nullable=True)
    previous_addresses = array(struct(Address), nullable=True)
```

Self-recursive schemas and recursive cycles across multiple schemas are rejected.

### Maps

`map(key_type, value_type, value_contains_null=True)` declares maps. Map keys must be Spark-supported key types and
are never nullable. Nested map values, including struct values, are supported.

```python
class Attribute(Schema):
    value = string(nullable=False)
    captured_at = timestamp(nullable=False)


class Product(Schema):
    attributes = map(string(), struct(Attribute), value_contains_null=False, nullable=True)
```

## Inheritance

Schema inheritance composes row contracts. It is useful for shared identifiers, audit fields, partition fields,
tenancy fields, and common source metadata; it is not arbitrary Python mixin behavior.

```python
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

The effective order is `id`, `tenant_id`, `created_at`, `updated_at`, `customer_id`, `total`. That order is used for
generated schemas, projections, strict validation, and generated documentation.

### Inheritance rules

- Direct schema bases are processed from left to right.
- Inherited fields precede local fields.
- A shared base in a diamond is collected once.
- A local redeclaration replaces an inherited field in its original position.
- An override replaces the whole field; metadata and descriptions are not merged.
- Deleting an inherited field is not supported.
- Two unrelated bases defining the same field require a local redeclaration.
- Non-schema mixins and polymorphic dispatch based on schema subclassing are not supported.

Resolve an ambiguous field locally:

```python
class SourceKeys(Schema):
    id = string(nullable=False)


class BusinessKeys(Schema):
    id = string(nullable=False)


class Order(SourceKeys, BusinessKeys):
    id = string(nullable=False)
    total = decimal(12, 2, nullable=True)
```

## Construct Outputs

Inside a compiled transform method, call a schema class to describe the output projection:

```python
return OrderNormalized(
    id=order.id,
    customer_id=lower(trim(order.customer_id)),
    total=to_decimal(order.total, precision=12, scale=2),
)
```

Rules:

- Positional arguments and unknown keywords are rejected.
- Every target field must be supplied, including nullable fields.
- Every non-nullable field must be supplied or safely copied.
- Keyword order may differ from declaration order.
- Generated projection order follows target schema order.
- Assignment type and nullability are checked before execution or generation.

For nested structs, construct the complete nested value:

```python
return OrderPublished(
    id=order.id,
    shipping=Address(
        city=trim(order.shipping.city),
        postal_code=order.shipping.postal_code,
    ),
)
```

Nested constructors produce Spark `struct(...)` expressions, not Python row objects or UDFs. Partial nested updates
are not supported; construct the whole nested value.

### Base overlays

Use `SchemaClass.base(...)` to copy inherited fields and overlay explicit values:

```python
return OrderWithCustomer.base(order)(
    customer_name=customer.name,
    customer_tier=customer.tier,
)
```

`base(...)` is valid only for a schema with direct schema bases. One base takes one compatible source row; multiple
bases take one source row per direct base, in declaration order. Extra source fields are ignored. Explicit overrides
win over copied fields. Unknown overrides, missing target fields, and incompatible copied fields are errors.

Use `project(...)` after `base(...)` when another source supplies still-unassigned compatible fields:

```python
return FulfillmentOption.base(demand).project(inventory)(
    available_to_promise=inventory.on_hand_quantity - inventory.reserved_quantity,
)
```

## Nullability and Assignment

Nullability is part of every field and expression.

- A nullable expression cannot feed a non-nullable target unless narrowed or repaired.
- `where(expr.is_not_null())` narrows simple field references after the filter in the same step method.
- A non-null parent struct can narrow nested reads according to nested field declarations.
- A `left` join makes right-side fields nullable; an `inner` join preserves their declared nullability.
- Hooks do not provide compile-time nullability facts unless a postcondition contract exists.
- Structure does not infer arbitrary predicate facts or scan data.

Narrow a value visibly:

```python
where(order.total.is_not_null())
return OrderNormalized(total=to_decimal(order.total, precision=12, scale=2))
```

Or repair it explicitly:

```python
return OrderNormalized(
    total=coalesce(to_decimal(order.total, precision=12, scale=2), 0),
)
```

The default ANSI assignment policy accepts exact matches, `None` for nullable fields, compatible numeric widening,
typed literals, and decimal widening that preserves integral digits and scale. It rejects nullable-to-non-nullable
assignment, implicit string parsing, numeric/string conversion, double-to-float, lossy decimal narrowing,
boolean/numeric conversion, and incompatible nested values.

Use explicit conversions when source and target meanings differ:

```python
return OrderNormalized(
    total=to_decimal(order.total, precision=12, scale=2),
    ordered_on=to_date(order.ordered_on, format="yyyy-MM-dd"),
    processed_at=to_timestamp(order.processed_at),
)
```

## Runtime Shape and Validation

Generated `*_SCHEMA` constants and execution result schemas contain effective Spark names, field order, data types,
nullability, and nested shape. They are shape-only artifacts:

```python
CUSTOMER_SCHEMA = T.StructType([
    T.StructField("id", T.StringType(), nullable=False),
    T.StructField("name", T.StringType(), nullable=True),
    T.StructField("tier", T.StringType(), nullable=True),
])
```

Callers may use a generated schema at a storage boundary:

```python
from structure_generated.store.pyspark.schemas.customer import *

customers = spark.read.schema(CUSTOMER_SCHEMA).parquet(customer_source_path)
```

Structure validates and projects DataFrames. Callers own reads, writes, table creation, partitioning, checkpoints,
output modes, and storage-specific options.

Validation runs at input, intermediate, and output boundaries. The modes are `off`, `schema_only`, and
`schema_and_constraints`:

```toml
[tool.structure]
input_validation_mode = "schema_only"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
output_validation_mode = "schema_only"
```

Schema-only validation checks required columns, unexpected columns in strict mode, configured order, Spark types,
reliable nullability metadata, nested struct shape, array element types, and map key/value types where available. It
does not call `count`, `collect`, `toPandas`, sample rows, or perform an aggregation.

`validate_intermediate = false` is a compatibility shorthand for `intermediate_validation_mode = "off"`. An explicit
mode wins; contradictory settings are configuration errors. Generated and direct execution validate at the same
boundaries.

When `plugin.pyspark.variant = "spark-connect"`, an omitted `validate_intermediate` resolves to `false` because each
intermediate schema lookup is a remote analysis request. Explicit configuration and transform-level settings remain
authoritative; input and final-output validation stay enabled.

## Schema Shape and Data Quality

Schemas describe row shape. Accepted values, ranges, patterns, uniqueness, referential checks, freshness, and row-count
rules are data-quality constraints and are not silently enabled by a schema declaration.

`schema_and_constraints` is explicit opt-in for declared constraints at eligible phases. Generated schemas remain
shape-only, and storage orchestration remains caller-owned.

## Diagnostics

Schema diagnostics identify the schema or field, the phase when relevant, the expected and actual type or nullability,
the problem, and the shortest correction.

```text
CompileError SCHEMA-E0304: Missing output field

Schema: OrderNormalized
Field: total

Use:
  Add total=... or copy it through OrderNormalized.base(source) when compatible.

See docs/background/Schema.back.md
```

```text
CompileError SCHEMA-E0302: Explicit conversion required

Output field: OrderNormalized.total: decimal(12, 2), nullable=True
Source expression: order.total: string(), nullable=True

Use:
  total=to_decimal(order.total, precision=12, scale=2)

See docs/background/Schema.back.md
```

```text
Ambiguous inherited field:
  Order.id is declared by SourceKeys and BusinessKeys.

Use a local declaration in Order:
  id = string(nullable=False)
```

## More Details

- [Schema background](../background/Schema.back.md) explains the model, rationale, and boundaries.
- [Schemas API](../api/Schemas.api.md) lists the compiler-visible declaration surface and target parity.
- [Schema Semantics](../dev/specifications/SchemaSemantics.md) is the normative developer specification.
- [Schema Declaration Syntax](../dev/specifications/SchemaDeclarationSyntax.md) defines accepted declarations.
- [Schema Inheritance](../dev/specifications/SchemaInheritance.md) defines inheritance resolution.
