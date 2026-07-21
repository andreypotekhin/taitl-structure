# Schema Declaration Syntax

## Purpose

Structure schemas declare the row contracts used by compiler checks, generated Spark `StructType` code, runtime schema
validation, traceability, and IDE navigation. The syntax must be explicit, readable, and cheap to inspect without importing
PySpark or creating a Spark session.

## Canonical Form

The canonical schema-module declaration form is:

```python
from structure import Schema
from structure.plugin.pyspark import *


class OrderRaw(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = string()


class OrderNormalized(Schema):
    id = string(nullable=False)
    customer_id = string(nullable=False)
    total = decimal(12, 2)
```

The field declaration has two visible parts:

1. A Python class attribute name, which becomes the Structure field name.
2. A field factory such as `string()` or `decimal(12, 2)`.

`from structure.plugin.pyspark import *` exposes the complete PySpark authoring DSL. Transform
modules that need an expression `array(...)` helper should import the PySpark DSL explicitly rather than combining
wildcard imports.

## Public Imports

The PySpark schema-field DSL is importable from `structure.plugin.pyspark`:

```python
from structure import Schema
from structure.plugin.pyspark import *
```

Standalone PySpark types for casts and special-function contracts are available through
`structure.plugin.pyspark.types`.

## Grammar

This is the accepted v1 schema declaration grammar in descriptive form:

```text
schema_class      := class NAME(Schema): field_decl+
field_decl        := NAME = field_factory(field_kwarg*)
field_factory     := string() | integer() | long() | float() | double() | boolean() | date() | timestamp()
                   | decimal(PRECISION, SCALE) | array(field_factory, contains_null=BOOL?)
                   | struct(schema_ref) | map(field_factory, field_factory, value_contains_null=BOOL?)
field_kwarg       := nullable=BOOL | alias=STRING | metadata=DICT | description=STRING
schema_ref        := Schema class object
```

The compiler should implement this grammar by inspecting actual runtime Schema objects, not by parsing source text
when import-based discovery is used. Source text or AST inspection may still be used for diagnostics and source spans.

## Field Rules

Every field factory accepts this common shape:

```python
string(
    *,
    nullable=True,
    alias=None,
    metadata=None,
    description=None,
)
```

Rules:

- `nullable` defaults to `True`.
- `alias` is an optional Spark column name for the
- `metadata` defaults to an empty immutable mapping.
- `description` is optional end-user documentation for generated docs, diagnostics, and traceability.
- Field declaration order is class body order.
- The attribute name is the Python field name.
- The Spark column name is `alias` when supplied, otherwise the Python field name.
- Aliases are schema-local. A later schema with the same Python field name does not inherit an alias unless it inherits
  the field definition through schema inheritance.
- Structure passes aliases through to Spark. It does not sanitize, normalize, or quote aliases for backend-specific
  identifier edge cases.
- v1 must reject duplicate Python field names and duplicate effective Spark column names after inherited fields are
  resolved.

Structure does not declare primary keys or uniqueness. A required field is expressed only with `nullable=False`.

## Type Rules

All schema type constructors return immutable value objects. Equality is structural.

### Scalar Types

The v1 scalar type constructors are:

```python
string()
integer()
long()
float()
double()
boolean()
date()
timestamp()
```

Generated PySpark mapping:

```text
string()     -> T.StringType()
integer()    -> T.IntegerType()
long()       -> T.LongType()
float()      -> T.FloatType()
double()     -> T.DoubleType()
boolean()    -> T.BooleanType()
date()       -> T.DateType()
timestamp()  -> T.TimestampType()
```

### Decimal

`decimal(precision, scale)` requires positive integer `precision` and non-negative integer `scale`.

Rules:

- `precision >= 1`
- `scale >= 0`
- `scale <= precision`
- v1 should reject omitted precision and scale.

Generated PySpark mapping:

```text
decimal(12, 2) -> T.DecimalType(12, 2)
```

### Array

`array(item_type, contains_null=True)` declares arrays.

Rules:

- `item_type` must be a Structure type object.
- `contains_null` defaults to `True`.
- Nested arrays are allowed.
- Arrays of structs are allowed with `array(struct(Address))`.

Generated PySpark mapping:

```text
array(string())                       -> T.ArrayType(T.StringType(), containsNull=True)
array(string(), contains_null=False)  -> T.ArrayType(T.StringType(), containsNull=False)
```

### Struct

`struct(schema)` declares a nested schema.

Rules:

- `schema` must be a `Schema` class, not an instance.
- Self-recursive schemas are rejected in v1.
- Recursive cycles across multiple schemas are rejected in v1.
- Nested struct field order follows the referenced schema class.

Generated PySpark mapping:

```text
struct(Address) -> T.StructType([...Address fields...])
```

### Map

`map(key_type, value_type, value_contains_null=True)` declares maps.

Rules:

- `key_type` and `value_type` must be Structure type objects.
- `key_type` must be a Spark-supported map key type.
- `value_contains_null` defaults to `True`.
- Map keys are never nullable because Spark map keys cannot be null.
- Nested map values are allowed.
- Map values may be structs with `map(string(), struct(Attribute))`.

Generated PySpark mapping:

```text
map(string(), string())  -> T.MapType(T.StringType(), T.StringType(), valueContainsNull=True)
```

## Schema Class Rules

A schema class is a class inheriting from `Structure` with `field(...)` attributes.

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

For schemas that extend earlier schema rows, a schema class may also start from one or more base rows and then overlay
explicit fields:

```python
return OrderWithCustomer.base(order)(
    customer_name=customer.name,
    customer_tier=customer.tier,
    customer_region=customer.region,
)
```

`SchemaClass.base(...)` is symbolic construction syntax, not a nested field and not a runtime row object. The compiler
expands it to the same explicit projection IR as the full constructor form. Generated PySpark remains an explicit
`select(...)` in target schema field order.

Base overlay rules:

- `SchemaClass.base(source)(...)` copies inherited target fields from `source` and applies explicit keyword overrides.
- Explicit keyword overrides always win over copied fields.
- Extra fields on a source row are ignored.
- Unknown override keywords are errors.
- Missing target fields are errors.
- Copied fields must be type- and nullability-compatible with the target field unless explicitly overridden.
- `SchemaClass.base(source)` without the second call is valid only when every target field can be copied safely.
- For a target schema with one direct schema base, `base(...)` takes one source row compatible with that base.
- For a target schema with multiple direct schema bases, `base(...)` takes one source row per direct schema base, in the
  same left-to-right order as the class declaration.
- Fields introduced locally by the target schema must be supplied as explicit overrides unless they can be copied by a
  later spec-defined default.
- Fields locally overriding inherited fields must be supplied explicitly; this keeps changed type, nullability,
  metadata, or meaning visible at the construction site.

Example with multiple schema bases:

```python
class OrderPublication(Schema):
    id = string(nullable=False)
    customer_name = string(nullable=True)
    total = decimal(12, 2, nullable=False)


class PublicationFlags(Schema):
    has_promotion = boolean(nullable=False)


class OrderPublished(OrderPublication, PublicationFlags):
    pass


flags = PublicationFlags(
    has_promotion=order.promotion_name.is_not_null(),
)


return OrderPublished.base(order, flags)
```

In this example, fields inherited through `OrderPublication` are copied from `order`, and fields inherited through
`PublicationFlags` are copied from `flags`. The source `order` may have extra fields from earlier enrichment stages;
only fields needed by `OrderPublication` are copied.

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
  id = string(nullable=False)

See docs/dev/specifications/SchemaDeclarationSyntax.md
```

```text
Invalid decimal type:
  OrderNormalized.total uses decimal(2, 12)

Decimal scale must be less than or equal to precision:
  total = decimal(12, 2, nullable=True)

See docs/dev/specifications/SchemaDeclarationSyntax.md
```

## Non-Goals

The following are not part of v1 canonical syntax:

- annotation-only field declarations such as `id: String`;
- dataclass-style defaults;
- Pydantic model inheritance as schema syntax;
- lowercase type sentinels such as `string`;
- implicit Spark type strings such as `"string"` or `"decimal(12,2)"`;
- non-schema mixins.

## Migration Notes

Existing examples using lowercase tokens should be migrated mechanically:

```text
field(string)          -> string()
field(decimal(12, 2))  -> decimal(12, 2)
field(boolean)         -> boolean()
field(integer)         -> integer()
field(long)            -> long()
field(float)           -> float()
field(double)          -> double()
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
12. Implement schema base overlay construction for inherited transform outputs.
13. Update docs and fixtures to canonical syntax.

## Acceptance Criteria

- `id = string(nullable=False)` is accepted.
- `total = decimal(12, 2, nullable=True)` is accepted.
- `ratio = float(nullable=True)` is accepted.
- `score = double(nullable=True)` is accepted.
- `items = array(string(), nullable=True)` is accepted.
- `address = struct(Address, nullable=True)` is accepted.
- `tags = map(string(), string(), nullable=True)` is accepted.
- `promotion_code = string(nullable=True, alias='promo-code')` is accepted.
- `id = field(string, nullable=False)` is rejected with a migration hint.
- `total = decimal(2, 12)` is rejected with a precision/scale diagnostic.
- Generated Spark schema code matches the declared field order.
- Generated Spark schema code uses field aliases as Spark column names.
- Aliases are schema-local except when a field definition is inherited.
- Schema-to-schema inheritance follows `SchemaInheritance.spec.md`.
- `SchemaClass.base(source)(overrides...)` constructs the same projection as an equivalent explicit constructor.
- `SchemaClass.base(source_a, source_b)(overrides...)` maps source rows to multiple direct schema bases in declaration
  order.
- `structure check` does not import PySpark only to inspect schema declarations.
- Public examples in `Readme.md` and `docs/` use explicit type objects.
