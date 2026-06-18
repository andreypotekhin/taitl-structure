# Schema Declaration Syntax

## Purpose

Structure schemas declare the row contracts used by compiler checks, generated Spark `StructType` code, runtime schema
validation, lineage, and IDE navigation. The syntax must be explicit, readable, and cheap to inspect without importing
PySpark or creating a Spark session.

## Canonical v1 Form

The v1 canonical schema declaration form is:

```python
from structure import Schema, field, String, Decimal


class OrderRaw(Schema):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    total = field(String(), nullable=True)


class OrderNormalized(Schema):
    id = field(String(), nullable=False)
    customer_id = field(String(), nullable=False)
    total = field(Decimal(12, 2), nullable=True)
```

The field declaration has three visible parts:

1. A Python class attribute name, which becomes the field name.
2. A `field(...)` call, which marks the attribute as a Structure field.
3. An explicit type object such as `String()` or `Decimal(12, 2)`.

Lowercase type sentinels such as `string`, `decimal(12, 2)`, and `boolean` are not canonical v1 syntax.

## Public Imports

The public schema DSL must be importable from `structure`:

```python
from structure import (
    Schema,
    field,
    String,
    Integer,
    Long,
    Float,
    Double,
    Decimal,
    Boolean,
    Date,
    Timestamp,
    Array,
    Struct,
    Map,
)
```

`Map` is part of the v1 schema type surface.

## Grammar

This is the accepted v1 schema declaration grammar in descriptive form:

```text
schema_class      := class NAME(Schema): field_decl+
field_decl        := NAME = field(type_expr, field_kwarg*)
type_expr         := scalar_type | decimal_type | array_type | struct_type | map_type
scalar_type       := String() | Integer() | Long() | Float() | Double() | Boolean() | Date() | Timestamp()
decimal_type      := Decimal(precision, scale)
array_type        := Array(type_expr, contains_null=BOOL?)
struct_type       := Struct(schema_ref)
map_type          := Map(key_type, value_type, value_contains_null=BOOL?)
field_kwarg       := nullable=BOOL | primary_key=BOOL | metadata=DICT | description=STRING
schema_ref        := Schema subclass object
```

The compiler should implement this grammar by inspecting actual runtime Structure objects, not by parsing source text
when import-based discovery is used. Source text or AST inspection may still be used for diagnostics and source spans.

## Field Rules

`field(...)` has this v1 shape:

```python
field(
    type_,
    *,
    nullable=True,
    primary_key=False,
    metadata=None,
    description=None,
)
```

Rules:

- `type_` is required and must be a Structure type object.
- `nullable` defaults to `True`.
- `primary_key` defaults to `False` and implies `nullable=False`.
- `metadata` defaults to an empty immutable mapping.
- `description` is optional end-user documentation for generated docs, diagnostics, and lineage.
- Field declaration order is class body order.
- The attribute name is the field name unless a future spec introduces aliases.
- v1 must reject duplicate field names after inherited fields are resolved.

`primary_key=True` on a nullable field is invalid unless `nullable=False` is explicitly supplied or inferred by the
implementation. Preferred compiler behavior is to normalize it to non-nullable and emit no warning.

## Type Rules

All schema type constructors return immutable value objects. Equality is structural.

### Scalar Types

The v1 scalar type constructors are:

```python
String()
Integer()
Long()
Float()
Double()
Boolean()
Date()
Timestamp()
```

Generated PySpark mapping:

```text
String()     -> T.StringType()
Integer()    -> T.IntegerType()
Long()       -> T.LongType()
Float()      -> T.FloatType()
Double()     -> T.DoubleType()
Boolean()    -> T.BooleanType()
Date()       -> T.DateType()
Timestamp()  -> T.TimestampType()
```

### Decimal

`Decimal(precision, scale)` requires positive integer `precision` and non-negative integer `scale`.

Rules:

- `precision >= 1`
- `scale >= 0`
- `scale <= precision`
- v1 should reject omitted precision and scale.

Generated PySpark mapping:

```text
Decimal(12, 2) -> T.DecimalType(12, 2)
```

### Array

`Array(item_type, contains_null=True)` declares arrays.

Rules:

- `item_type` must be a Structure type object.
- `contains_null` defaults to `True`.
- Nested arrays are allowed.
- Arrays of structs are allowed with `Array(Struct(Address))`.

Generated PySpark mapping:

```text
Array(String())                       -> T.ArrayType(T.StringType(), containsNull=True)
Array(String(), contains_null=False)  -> T.ArrayType(T.StringType(), containsNull=False)
```

### Struct

`Struct(schema)` declares a nested schema.

Rules:

- `schema` must be a `Schema` subclass, not an instance.
- Self-recursive schemas are rejected in v1.
- Recursive cycles across multiple schemas are rejected in v1.
- Nested struct field order follows the referenced schema class.

Generated PySpark mapping:

```text
Struct(Address) -> T.StructType([...Address fields...])
```

### Map

`Map(key_type, value_type, value_contains_null=True)` declares maps.

Rules:

- `key_type` and `value_type` must be Structure type objects.
- `key_type` must be a Spark-supported map key type.
- `value_contains_null` defaults to `True`.
- Map keys are never nullable because Spark map keys cannot be null.
- Nested map values are allowed.
- Map values may be structs with `Map(String(), Struct(Attribute))`.

Generated PySpark mapping:

```text
Map(String(), String())  -> T.MapType(T.StringType(), T.StringType(), valueContainsNull=True)
```

## Schema Class Rules

A schema class is a class inheriting from `Schema` with `field(...)` attributes.

Rules:

- Schema classes are declarative contracts, not data classes.
- Schema constructors are used in transform methods to capture output projections.
- User-defined non-field class attributes are allowed only if they do not look like failed field declarations.
- Public schema classes should be import-safe.
- Schema-to-schema inheritance is supported by `SchemaInheritance.spec.md`.

## Output Construction

Inside compiled transform methods, calling a schema class constructs a symbolic output record:

```python
return OrderNormalized(
    id=order.id,
    total=to_decimal(order.total, precision=12, scale=2),
)
```

Rules:

- All non-nullable output fields must be supplied unless defaults are introduced by a later spec.
- Unknown keyword arguments are errors.
- Missing nullable fields are errors in v1. Developers should be explicit to keep generated projections reviewable.
- Positional arguments are rejected.
- Field keyword order may differ from declaration order; generated projection order follows schema declaration order.

## Diagnostics

Schema declaration diagnostics must include:

- schema class name;
- field name when available;
- source file and line when available;
- the invalid value or syntax shape;
- a concise fix.

Examples:

```text
Invalid schema field type:
  OrderRaw.id uses string

Use an explicit Structure type object:
  id = field(String(), nullable=False)

See docs/specifications/SchemaDeclarationSyntax.md
```

```text
Invalid decimal type:
  OrderNormalized.total uses Decimal(2, 12)

Decimal scale must be less than or equal to precision:
  total = field(Decimal(12, 2), nullable=True)

See docs/specifications/SchemaDeclarationSyntax.md
```

## Non-Goals

The following are not part of v1 canonical syntax:

- annotation-only field declarations such as `id: String`;
- dataclass-style defaults;
- Pydantic model inheritance as schema syntax;
- lowercase type sentinels such as `string`;
- implicit Spark type strings such as `"string"` or `"decimal(12,2)"`;
- field aliases;
- non-schema mixins.

## Migration Notes

Existing examples using lowercase tokens should be migrated mechanically:

```text
field(string)          -> field(String())
field(decimal(12, 2))  -> field(Decimal(12, 2))
field(boolean)         -> field(Boolean())
field(integer)         -> field(Integer())
field(long)            -> field(Long())
field(float)           -> field(Float())
field(double)          -> field(Double())
```

The compiler may include a temporary compatibility mode for lowercase aliases during early implementation, but docs,
fixtures, and generated examples must use only the canonical explicit type-object form.

## Implementation Checklist

1. Add immutable schema type objects.
2. Export type constructors from `structure`.
3. Implement `field(...)` metadata capture.
4. Preserve class-body field order.
5. Resolve schema inheritance.
6. Build `SchemaDef` and `FieldDef` from schema classes.
7. Reject lowercase sentinels and non-Structure type values.
8. Validate decimal precision and scale.
9. Validate array and struct nested type expressions.
10. Generate deterministic Spark `StructType` code.
11. Add diagnostics that link to this specification.
12. Update docs and fixtures to canonical syntax.

## Acceptance Criteria

- `id = field(String(), nullable=False)` is accepted.
- `total = field(Decimal(12, 2), nullable=True)` is accepted.
- `ratio = field(Float(), nullable=True)` is accepted.
- `score = field(Double(), nullable=True)` is accepted.
- `items = field(Array(String()), nullable=True)` is accepted.
- `address = field(Struct(Address), nullable=True)` is accepted.
- `tags = field(Map(String(), String()), nullable=True)` is accepted.
- `id = field(string, nullable=False)` is rejected with a migration hint.
- `total = field(Decimal(2, 12))` is rejected with a precision/scale diagnostic.
- Generated Spark schema code matches the declared field order.
- Schema-to-schema inheritance follows `SchemaInheritance.spec.md`.
- `structure check` does not import PySpark only to inspect schema declarations.
- Public examples in `Readme.md` and `docs/` use explicit type objects.
