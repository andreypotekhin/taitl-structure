# Schema Reference

Schemas define Structure's typed row contracts. They drive compiler checks, generated Spark schemas, execution,
runtime validation, diagnostics, traceability, generated code, and generated documentation. A schema is a declarative
contract, not a data class, a Python row object, or a raw PySpark `StructType`.

The schema model is the source of truth. Generated PySpark schemas and execution-materialized schemas are derived
artifacts.

## Semantic Contract

Structure schema behavior has four layers:

1. Source declarations: Python classes that inherit `Schema` and declare fields with `field(...)`.
2. Compiler model: backend-neutral `SchemaDef`, `FieldDef`, and type values.
3. Runtime shape: generated or materialized Spark `StructType` values.
4. Value constraints: explicit data-quality rules outside the base shape model.

Schema extraction, type validation, inheritance resolution, and compiler checks are Spark-free. They must not import
PySpark, start Java, create a `SparkSession`, or inspect live data.

### Canonical Declaration

The canonical v1 declaration form is explicit:

```python
from structure import *


class OrderRaw(Schema):
    id = field(String(), nullable=False, primary_key=True)
    customer_id = field(String(), nullable=False)
    total = field(String(), nullable=True)


class OrderNormalized(Schema):
    id = field(String(), nullable=False, primary_key=True)
    customer_id = field(String(), nullable=False)
    total = field(Decimal(12, 2), nullable=True)
```

Rules:

- Every field uses `field(type_, ...)`.
- Every type is an explicit immutable Structure type object.
- Field order is class-body order after inheritance is resolved.
- Field names are Python attribute names.
- `primary_key=True` implies `nullable=False`.
- Public examples must use this form.

### Schema Identity

A schema class defines a named row contract. Two schemas with identical fields may be structurally compatible, but they
are not the same schema identity.

Rules:

- `SchemaDef.qualified_name` is the stable compiler identity for a schema class.
- Source path and line number are diagnostic metadata, not semantic identity.
- Renaming a schema class or moving it to another module changes identity.
- Generated schema constant names are derived deterministically from schema identity and local naming rules.

Example:

```python
class OrderRaw(EntityKeys):
    pass


class CustomerRaw(EntityKeys):
    pass
```

`OrderRaw` and `CustomerRaw` may have compatible field structure, but they are different schema identities. Transform
flow validation uses schema identity unless a compatibility rule explicitly asks for structural compatibility.

## Declaration Syntax

Structure schemas declare row contracts in ordinary Python class bodies. The syntax is intentionally cheap to inspect
for compiler checks, generated Spark `StructType` code, runtime validation, traceability, and IDE navigation.

### Public Imports

The public schema DSL is importable from `structure`:

```python
from structure import *
```

`Map` is part of the schema type surface.

### Grammar

In descriptive form, the accepted schema declaration grammar is:

```text
schema_class      := class NAME(Schema): field_decl*
field_decl        := NAME = field(type_expr, field_kwarg*)
type_expr         := scalar_type | decimal_type | array_type | struct_type | map_type
scalar_type       := String() | Integer() | Long() | Float() | Double() | Boolean() | Date() | Timestamp()
decimal_type      := Decimal(precision, scale)
array_type        := Array(type_expr, contains_null=BOOL?)
struct_type       := Struct(schema_ref)
map_type          := Map(key_type, value_type, value_contains_null=BOOL?)
field_kwarg       := nullable=BOOL | primary_key=BOOL | alias=STRING | metadata=DICT | description=STRING
schema_ref        := Schema class object
```

The compiler should implement this grammar by inspecting runtime schema objects when import-based discovery is used.
Source text or AST inspection may still be used for diagnostics and source spans.

Lowercase type sentinels, annotation-only declarations, dataclass-style defaults, raw PySpark fields, implicit Spark
type strings, and non-schema mixins are outside the canonical form.

### Schema Classes

A schema class is a class inheriting from `Schema` with `field(...)` attributes.

Rules:

- Schema classes are declarative contracts, not data classes.
- Schema constructors are used in transform methods to capture output projections.
- User-defined non-field class attributes are allowed only if they do not look like failed field declarations.
- Public schema classes should be import-safe.
- A schema class may inherit directly from `Schema`.
- A schema class may inherit from one or more user-defined schema classes.
- All non-`object` bases of a schema class must be schema classes.
- Python must be able to construct a valid C3 MRO for the class.

### Fields

`field(...)` has this v1 shape:

```python
field(
    type_,
    *,
    nullable=True,
    primary_key=False,
    alias=None,
    metadata=None,
    description=None,
)
```

`type_` is required and must be a Structure type. `nullable` defaults to `True`. `primary_key` defaults to `False` and
implies `nullable=False`. `alias` is the Spark column name; otherwise the Python attribute name is also the Spark name.
`metadata` is an immutable mapping. `description` feeds generated documentation, diagnostics, and traceability.

Each effective field records:

```text
name
type
nullable
primary_key
alias
metadata
description
declaring_schema
owning_schema
inherited
overrides
source location
```

Rules:

- Field declaration order is class-body order after inheritance resolves.
- Field order is the order of generated fields, projections, runtime validation, and documentation.
- Python field names and effective Spark column names must each be unique.
- Aliases are schema-local except when the field itself is inherited.
- Structure passes aliases to Spark unchanged; it does not sanitize, normalize, or quote backend-specific identifiers.
- Unknown field constructor keywords are declaration errors.
- Field metadata and descriptions do not change Spark shape semantics unless a narrower spec says so.

`primary_key=True` on a nullable field is invalid unless `nullable=False` is explicitly supplied or inferred by the
implementation. Preferred compiler behavior is to normalize it to non-nullable and emit no warning.

### Field Options In Use

This declaration uses every field option in its ordinary role:

```python
class CustomerSource(Schema):
    customer_id = field(
        String(),
        nullable=False,
        primary_key=True,
        alias="customer-id",
        metadata={"source": "crm", "pii": "indirect"},
        description="Stable customer identifier supplied by the CRM.",
    )
    display_name = field(
        String(),
        nullable=True,
        description="Name displayed to account users.",
    )
```

`customer_id` is the Python name used in Structure code; `customer-id` is the column that Spark reads and writes.
`primary_key=True` makes the field non-nullable, but it does not scan for or enforce uniqueness. Uniqueness is a
data-quality constraint, not a schema-shape property. `metadata` and `description` travel with the compiler model for
generated documentation, diagnostics, and traceability; neither changes the Spark type or validates data values.

### Field Aliases

Use `alias=...` when the physical Spark column name is not the Python field name. This is common for source columns
with spaces, hyphens, reserved words, leading digits, or legacy names that should not leak into Python code.

```python
class OrderRaw(Schema):
    id = field(String(), nullable=False, primary_key=True)
    promotion_code = field(String(), nullable=True, alias="promo-code")
    customer_id = field(String(), nullable=True, alias="customer id")
    class_ = field(String(), nullable=True, alias="class")
    field_1st_code = field(String(), nullable=True, alias="1st code")
```

The Python name remains the Structure field name:

```python
return OrderNormalized(
    id=order.id,
    promotion_code=lower(trim(order.promotion_code)),
    customer_id=lower(trim(order.customer_id)),
)
```

The Spark name is `alias` when supplied:

```python
ORDER_RAW_SCHEMA = T.StructType([
    T.StructField("id", T.StringType(), nullable=False),
    T.StructField("promo-code", T.StringType(), nullable=True),
    T.StructField("customer id", T.StringType(), nullable=True),
    T.StructField("class", T.StringType(), nullable=True),
    T.StructField("1st code", T.StringType(), nullable=True),
])
```

Aliases are schema-local. A later schema with the same Python field name does not inherit the alias unless it inherits
the field definition:

```python
class RawPromotion(Schema):
    promotion_code = field(String(), nullable=True, alias="promo-code")


class NormalizedPromotion(Schema):
    promotion_code = field(String(), nullable=True)


class StillRawPromotion(RawPromotion):
    pass
```

Effective Spark column names:

```text
RawPromotion.promotion_code         -> "promo-code"
NormalizedPromotion.promotion_code  -> "promotion_code"
StillRawPromotion.promotion_code    -> "promo-code"
```

Invalid aliases fail early:

```python
field(String(), alias="")    # rejected: empty alias
field(String(), alias=123)   # rejected: alias is not a string
```

Duplicate effective Spark column names are rejected after aliases and inheritance are resolved:

```python
class Duplicate(Schema):
    promotion_code = field(String(), alias="promo-code")
    alternate_code = field(String(), alias="promo-code")  # rejected
```

Schema field `alias=...` is separate from APIs named `.alias(...)`:

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized).alias("orders")
```

Here `.alias("orders")` names a transform output for composition and result lookup. It does not rename schema fields or
Spark columns. Generated PySpark may also use Spark's own `.alias(...)` when rendering projections:

```python
orders.select(
    F.col("promo-code").alias("promotion_code"),
)
```

That generated Spark alias is an implementation detail of projection rendering. In schema declarations, use
`field(..., alias="spark-column-name")`.

### Unsupported Syntax

The following are not part of v1 canonical syntax:

- annotation-only field declarations such as `id: String`;
- dataclass-style defaults;
- Pydantic model inheritance as schema syntax;
- lowercase type sentinels such as `string`;
- implicit Spark type strings such as `"string"` or `"decimal(12,2)"`;
- non-schema mixins.

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

## Types

All schema type constructors return immutable value objects. Equality is structural.

```text
String() == String()
Decimal(12, 2) == Decimal(12, 2)
Array(String()) == Array(String())
Struct(Address) == Struct(Address)
```

The v1 type model is:

```text
StringType
IntegerType
LongType
FloatType
DoubleType
DecimalType(precision, scale)
BooleanType
DateType
TimestampType
ArrayType(item_type, contains_null)
StructType(schema)
MapType(key_type, value_type, value_contains_null)
```

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

One schema can use all scalar types. The choice is part of the contract; Structure does not infer a field type from
live data:

```python
class ScalarSample(Schema):
    label = field(String(), nullable=False)
    item_count = field(Integer(), nullable=False)
    event_sequence = field(Long(), nullable=False)
    confidence = field(Float(), nullable=True)
    score = field(Double(), nullable=True)
    active = field(Boolean(), nullable=False)
    business_date = field(Date(), nullable=False)
    recorded_at = field(Timestamp(), nullable=False)
```

### Decimal

`Decimal(precision, scale)` requires an integer precision of at least one and a scale from zero through that precision.

Rules:

- `precision >= 1`
- `scale >= 0`
- `scale <= precision`
- v1 should reject omitted precision and scale.

Generated PySpark mapping:

```text
Decimal(12, 2) -> T.DecimalType(12, 2)
```

For example, a currency field with up to ten digits before the decimal point and two after it is declared as:

```python
class Payment(Schema):
    total = field(Decimal(12, 2), nullable=False)
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

For example, `tags` may be absent as a whole, while a present tag list may not contain null elements:

```python
class TaggedOrder(Schema):
    tags = field(Array(String(), contains_null=False), nullable=True)
```

### Struct

`Struct(schema)` declares a nested schema.

Rules:

- `schema` must be a `Schema` class, not an instance.
- `Struct(Address)` identifies a particular schema class.
- `Struct(Address)` and `Struct(BillingAddress)` are compatible only when they reference the same schema class.
- Nested struct field order follows the referenced schema class.
- `Struct(SchemaClass)` uses the effective inherited field set of `SchemaClass`.
- Self-recursive schemas are rejected in v1.
- Recursive cycles across multiple schemas are rejected in v1.

Example:

```python
class AddressBase(Schema):
    city = field(String(), nullable=True)


class ShippingAddress(AddressBase):
    postal_code = field(String(), nullable=True)


class Order(Schema):
    shipping = field(Struct(ShippingAddress), nullable=True)
    previous_addresses = field(Array(Struct(ShippingAddress)), nullable=True)
```

Generated Spark schema for both fields includes `city` and `postal_code`; `previous_addresses` is an array whose
elements use that same nested shape.

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
- Higher-order map transformations remain a v2 expression feature.

Generated PySpark mapping:

```text
Map(String(), String())  -> T.MapType(T.StringType(), T.StringType(), valueContainsNull=True)
```

For example, a sparse set of typed attributes can use struct values and require every present value to be non-null:

```python
class Attribute(Schema):
    value = field(String(), nullable=False)
    captured_at = field(Timestamp(), nullable=False)


class Product(Schema):
    attributes = field(
        Map(String(), Struct(Attribute), value_contains_null=False),
        nullable=True,
    )
```

### Spark Type Mapping

Full v1 mapping:

```text
String()              -> T.StringType()
Integer()             -> T.IntegerType()
Long()                -> T.LongType()
Float()               -> T.FloatType()
Double()              -> T.DoubleType()
Decimal(12, 2)        -> T.DecimalType(12, 2)
Boolean()             -> T.BooleanType()
Date()                -> T.DateType()
Timestamp()           -> T.TimestampType()
Array(String())       -> T.ArrayType(T.StringType(), containsNull=True)
Struct(Address)       -> T.StructType([...])
Map(String(), Long()) -> T.MapType(T.StringType(), T.LongType(), valueContainsNull=True)
```

Spark schema generation must be deterministic and formatted consistently. When a field has an alias, Spark schema
generation uses the alias as the `StructField` name.

## Inheritance

Schema inheritance is ordered schema-to-schema field composition. It is especially useful for shared identifiers, audit
columns, partition columns, tenancy fields, and common source metadata. It is not arbitrary Python mixin behavior.

### Canonical Form

```python
from structure import *


class EntityKeys(Schema):
    id = field(String(), nullable=False, primary_key=True)
    tenant_id = field(String(), nullable=False)


class AuditFields(Schema):
    created_at = field(Timestamp(), nullable=False)
    updated_at = field(Timestamp(), nullable=True)


class Order(EntityKeys, AuditFields):
    customer_id = field(String(), nullable=False)
    total = field(Decimal(12, 2), nullable=True)
```

Effective field order for `Order` is:

```text
id
tenant_id
created_at
updated_at
customer_id
total
```

This is the generated schema, projection, strict-validation, and documentation order.

### Supported Inheritance

Examples:

```python
class Customer(EntityKeys):
    name = field(String(), nullable=True)
```

```python
class Order(EntityKeys, AuditFields):
    total = field(Decimal(12, 2), nullable=True)
```

Non-schema mixins are not supported in v1:

```python
class Order(EntityKeys, SomePlainMixin):  # rejected
    total = field(Decimal(12, 2), nullable=True)
```

### Field Collection Algorithm

The compiler builds an effective ordered field map for each schema class.

Algorithm:

1. Start with an empty ordered map.
2. Visit direct schema bases from left to right as written in the class definition.
3. For each base, recursively collect that base's effective fields before collecting later bases.
4. Skip a schema base that was already visited through a diamond inheritance path.
5. Add local fields in class-body declaration order.
6. If a local field has the same name as an inherited field, replace the inherited field in the same position.
7. If a local field is new, append it after all inherited fields.

This gives users predictable field order: fields from the first base appear before fields from the second base, and
local fields appear last.

### Overrides

A schema class may override an inherited field by redeclaring the same field name.

```python
class SoftDeleteFields(Schema):
    deleted_at = field(Timestamp(), nullable=True)


class RequiredDeleteMarker(SoftDeleteFields):
    deleted_at = field(Timestamp(), nullable=False)
```

Override rules:

- Override replacement is whole-field replacement.
- Override position is the inherited field position.
- Type, nullability, primary key flag, metadata, and description all come from the overriding field.
- Metadata is not merged.
- Description is not merged.
- Overriding a field with a non-field value is rejected.
- Deleting an inherited field is not supported in v1.

Whole-field replacement keeps behavior visible. A reader can inspect the overriding line and know the final field
definition.

### Duplicate Fields Across Bases

If two unrelated bases define the same field name, the subclass must redeclare that field locally to resolve the
ambiguity.

Rejected:

```python
class SourceKeys(Schema):
    id = field(String(), nullable=False)


class BusinessKeys(Schema):
    id = field(String(), nullable=False, primary_key=True)


class Order(SourceKeys, BusinessKeys):
    total = field(Decimal(12, 2), nullable=True)
```

Accepted:

```python
class Order(SourceKeys, BusinessKeys):
    id = field(String(), nullable=False, primary_key=True)
    total = field(Decimal(12, 2), nullable=True)
```

The resolved `id` keeps the first inherited position. In the accepted example, `id` remains before `total`.

Diamond inheritance through a shared base is not a duplicate:

```python
class Keys(Schema):
    id = field(String(), nullable=False)


class CustomerKeys(Keys):
    customer_id = field(String(), nullable=False)


class ProductKeys(Keys):
    product_id = field(String(), nullable=False)


class CustomerProduct(CustomerKeys, ProductKeys):
    score = field(Decimal(8, 4), nullable=True)
```

`id` is collected once because `Keys` is a shared ancestor.

### Field Origin

The schema model must retain field origin information.

For every effective field, `FieldDef` records:

- final owning schema;
- declaring schema;
- field name;
- effective order;
- whether it was inherited;
- whether it overrides another field;
- the overridden field origin when applicable.

This information is used for diagnostics, generated documentation, traceability, and source navigation.

### Inheritance Non-Goals

The following are not part of v1:

- deleting inherited fields;
- partial field overrides;
- metadata merging;
- description merging;
- non-schema mixins;
- changing field order locally without redeclaring the full schema;
- polymorphic transform dispatch based on schema subclassing.

## Compiler Model

The schema model represents user-declared data structures independently from PySpark.

### Core Model

```text
SchemaDef
  name
  qualified_name
  module
  source_path
  source_line
  bases
  fields
  local_fields
  constraints
  metadata

FieldDef
  name
  type
  nullable
  primary_key
  alias
  metadata
  description
  declaring_schema
  owning_schema
  inherited
  overrides
  source_path
  source_line

TypeDef
  kind
  parameters
```

`SchemaDef.fields` is the effective ordered field list after inheritance resolution. `SchemaDef.local_fields` contains
only fields declared directly on the schema class.

### SchemaDef Rules

`SchemaDef` represents one discovered `Schema` class.

Rules:

- `name` is the class name.
- `qualified_name` is the importable module-qualified class name.
- `module` is the source module name.
- `source_path` and `source_line` are included when available.
- `bases` lists direct schema bases in class definition order.
- `fields` contains effective fields in generated output order.
- `local_fields` contains fields declared directly on the class.
- `constraints` contains schema-level constraints owned by the data-quality constraint model.
- `metadata` is immutable.

### FieldDef Rules

`FieldDef` represents one effective schema field.

Rules:

- `name` is the Python class attribute name.
- `type` is a Structure `TypeDef`.
- `nullable` defaults to `True`.
- `primary_key` defaults to `False`.
- `primary_key=True` implies `nullable=False`.
- `alias` is an optional Spark column name. If absent, the Spark column name is `name`.
- `metadata` is immutable and defaults to empty.
- `description` is optional.
- `declaring_schema` is the schema class that declared the effective field.
- `owning_schema` is the schema whose effective field list contains this field.
- `inherited` is true when `declaring_schema != owning_schema`.
- `overrides` points to the overridden field origin when the field replaces an inherited field.

Field order is part of the schema contract. Generated Spark schemas and projections must use `SchemaDef.fields` order.
Python constructors, symbolic field access, diagnostics, and compiler checks use `name`; Spark schemas, validation,
expression rendering, and projection output use `alias or name`.

### Extraction Flow

```text
Schema class
  -> local field capture
  -> inheritance resolution
  -> type validation
  -> SchemaDef
  -> compile-time checks
  -> generated Spark schema
  -> runtime validation
```

Schema extraction must reject:

- non-Structure type values in `field(...)`;
- invalid decimal precision or scale;
- invalid nested type expressions;
- recursive struct cycles;
- ambiguous inherited fields;
- non-schema bases;
- duplicate effective field names after inheritance resolution;
- empty or non-string field aliases;
- duplicate effective Spark column names after aliases and inheritance are resolved;
- unsupported field declaration shapes.

Errors should link to the most specific relevant section.

### Performance

Schema extraction should be cacheable by source fingerprint.

Targets:

- extraction should not import PySpark, start Java, create a SparkSession, or contact a Spark cluster;
- type objects should be lightweight immutable values;
- inheritance resolution should be linear in the number of schema classes plus field declarations;
- generated Spark `StructType` text should be deterministic and cheap to emit.

## Output Construction

Inside a compiled transform method, calling a schema class creates a symbolic projection into that schema:

```python
return OrderNormalized(
    id=order.id,
    customer_id=lower(trim(order.customer_id)),
    total=to_decimal(order.total, precision=12, scale=2),
)
```

Rules:

- Positional arguments are rejected.
- Unknown keyword arguments are errors.
- Missing nullable fields are errors in v1. Developers should be explicit to keep generated projections reviewable.
- All non-nullable output fields must be supplied unless defaults are introduced by a later spec.
- Field keyword order may differ from declaration order.
- Generated projection order follows target schema declaration order, not source keyword order.
- Assignment type and nullability are checked before generated or direct runtime execution.

### Nested Struct Construction

For nested `Struct(...)` fields, use the nested schema constructor as the assigned value:

```python
return OrderPublished(
    id=order.id,
    shipping=Address(
        city=trim(order.shipping.city),
        postal_code=order.shipping.postal_code,
    ),
)
```

Rules:

- Nested constructors lower to Spark `struct(...)` expressions, not Python objects or UDFs.
- The nested constructor must assign every field declared by the nested schema.
- The constructed schema must match the target `Struct(...)` schema identity.
- To change one child field, construct the whole nested value for now.
- Partial nested updates such as replacing only `shipping.city` are deferred planned work.

### Base Overlay Construction

For schemas that extend earlier schema rows, a schema class may start from one or more base rows and then overlay
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
- The compiler maps fields by inherited field origin, not by searching all sources for a matching field name.
- Fields introduced locally by the target schema must be supplied as explicit overrides unless they can be copied by a
  later spec-defined default.
- Fields locally overriding inherited fields must be supplied explicitly; this keeps changed type, nullability,
  metadata, or meaning visible at the construction site.

Example with multiple schema bases:

```python
class OrderPublication(Schema):
    id = field(String(), nullable=False, primary_key=True)
    customer_name = field(String(), nullable=True)
    total = field(Decimal(12, 2), nullable=False)


class PublicationFlags(Schema):
    has_promotion = field(Boolean(), nullable=False)


class OrderPublished(OrderPublication, PublicationFlags):
    pass


flags = PublicationFlags(
    has_promotion=order.promotion_name.is_not_null(),
)


return OrderPublished.base(order, flags)
```

In this example, fields inherited through `OrderPublication` are copied from `order`, and fields inherited through
`PublicationFlags` are copied from `flags`. The `order` row may have extra fields from earlier enrichment stages; only
fields required by `OrderPublication` are copied.

## Nullability And Assignment

Nullability is part of every field and every expression.

Rules:

- A nullable expression cannot feed a non-nullable target unless narrowed or repaired.
- `where(expr.is_not_null())` narrows simple field references after the filter in the same step method.
- `where(parent_struct.is_not_null())` narrows nested reads through that parent according to each nested field's own
  declared nullability.
- `Join.LEFT` makes joined right-side fields nullable after the join.
- `Join.INNER` preserves right-side declared nullability unless later operations narrow it.
- Hooks do not provide compile-time nullability facts unless a later hook postcondition contract exists.

Do not declare a field non-null merely because source data is expected to be clean. Narrow a direct field in the same
step with a visible filter:

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

Structure does not infer arbitrary predicate facts, scan data, or assume that hooks establish nullability facts.

### Assignment Rules

The default ANSI policy accepts exact matches, `None` for nullable fields, compatible numeric widening, typed literals,
and decimal widening that preserves both integer digits and scale. It rejects nullable-to-non-nullable assignment,
implicit string parsing, numeric/string conversion, double-to-float, lossy decimal narrowing, boolean/numeric
conversion, and incompatible nested values.

`Decimal(p1, s1)` assigns to `Decimal(p2, s2)` only if `s2 >= s1` and `p2 - s2 >= p1 - s1`. A 32-bit integer needs at
least ten integral decimal digits; a long needs nineteen. `coalesce` computes a least common type, so an untyped `0`
can become `Decimal(12, 2)` when its other argument and output target establish that context.

Use explicit semantic parsing conversions for string data:

```python
return OrderNormalized(
    total=to_decimal(order.total, precision=12, scale=2),
    ordered_on=to_date(order.ordered_on, format="yyyy-MM-dd"),
    processed_at=to_timestamp(order.processed_at),
)
```

Ordinary compatible assignments stay compact. Here an integer field widens to `Long()`, `None` supplies a nullable
field, and parsing remains explicit where the source and target have different meaning:

```python
class OrderRaw(Schema):
    item_count = field(Integer(), nullable=False)
    total = field(String(), nullable=True)


class OrderNormalized(Schema):
    item_count = field(Long(), nullable=False)
    total = field(Decimal(12, 2), nullable=True)
    rejection_reason = field(String(), nullable=True)


def normalize(self, order: OrderRaw) -> OrderNormalized:
    return OrderNormalized(
        item_count=order.item_count,
        total=to_decimal(order.total, precision=12, scale=2),
        rejection_reason=None,
    )
```

## Runtime Shape And Validation

Generated `*_SCHEMA` constants and execution result schemas are equivalent shape-only `StructType` artifacts. They include
effective Spark names, order, data types, nullability, and nested shape.

Example:

```python
class Customer(Schema):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    tier = field(String(), nullable=True)
```

Generated PySpark schema:

```python
CUSTOMER_SCHEMA = T.StructType([
    T.StructField("id", T.StringType(), nullable=False),
    T.StructField("name", T.StringType(), nullable=True),
    T.StructField("tier", T.StringType(), nullable=True),
])
```

The generated constant is an ordinary PySpark `StructType`, so callers may use it at a storage boundary:

```python
from structure_generated.orders.pyspark.schemas.customer import CUSTOMER_SCHEMA

customers = spark.read.schema(CUSTOMER_SCHEMA).parquet(customer_source_path)
```

Structure validates and projects DataFrames, but the caller owns reads, writes, table creation, partitioning,
checkpoints, and storage-specific options.

Callers may use generated schemas with `spark.read.schema(...)`, their own validation, and pre-write projection. They
do not execute value-level constraints.

Execution must materialize equivalent Spark schemas from `SchemaDef.fields` and expose them from the transform
invocation after `run(session)`. This gives direct-runtime callers the same shape contract without requiring generated files.

### Validation Phases

Runtime validation has input, intermediate, and output phases.

- Input validation checks supplied DataFrames.
- Intermediate validation checks every compiled step and its attached hooks.
- Output validation checks final returned frames.

The validation modes are `off`, `schema_only`, and `schema_and_constraints`. The default policy is schema-only at all
phases, with intermediate validation enabled:

```toml
[tool.structure]
input_validation_mode = "schema_only"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
output_validation_mode = "schema_only"
```

`validate_intermediate = false` is a compatibility shorthand for `intermediate_validation_mode = "off"`. If both are
set, the explicit mode wins and a contradiction is a configuration error. Policy resolves from defaults through project
configuration and CLI flags, then transform, method, and hook-local overrides.

### Schema-Only Validation

Schema-only validation checks:

- required columns;
- unexpected columns in strict mode;
- target order where required;
- Spark types;
- reliable nullability metadata;
- nested struct shape;
- array element type where available;
- map key and value types where available.

Schema-only validation never calls `count`, `collect`, `toPandas`, samples rows, or performs a data aggregation. Input
validation precedes the first step; step and hook validation occurs at its declared boundary; final validation precedes
the result. Streaming DataFrames can use schema-only validation because it inspects metadata.

A hook may opt into `SchemaMode.ALLOW_EXTRA_COLUMNS`; `project_output=True` then restores target columns and order.
Execution and generated-code execution validate at identical boundaries.

## Data Quality Boundary

Schema shape and data quality are distinct. Accepted values, ranges, patterns, decimal domains, uniqueness,
referential checks, freshness, and row-count rules are value or dataset facts that can require filters, aggregation,
joins, or actions. They are explicit, phase-bound, and cost-classified rather than silently enabled by a schema
declaration.

`schema_and_constraints` reserves opt-in validation for such checks. Until concrete families are implemented, it may
report that only schema checks are available. Generated schemas remain shape-only; future constraint metadata must use
separate artifacts unless a later design explicitly changes this contract. Each data-quality constraint needs its own
streaming admission rule.

Opt in at the phases where the extra Spark work is intended:

```toml
[tool.structure]
input_validation_mode = "schema_and_constraints"
intermediate_validation_mode = "schema_and_constraints"
output_validation_mode = "schema_and_constraints"
```

This configuration does not invent a constraint. It merely permits explicitly declared, phase-eligible constraints to
run when that public DSL is available.

Planned field-local constraint families are accepted values, numeric and temporal ranges, patterns, length limits, and
decimal domains. Schema-level families are unique keys, cross-field conditions, row-count bounds, and freshness.
Cross-dataset families are referential and anti-existence checks.

Each constraint must declare its target, phases, kind, severity, source location, and cost: compile-time only,
schema-only, row-local, aggregation-based, or join-based. Cost determines default eligibility and streaming support.
Storage orchestration remains caller-owned even where Structure supplies generated schemas for caller-owned reads and
writes.

## Diagnostics

Schema diagnostics name the schema, field, phase, source expression or DataFrame column, applicable target policy, the
problem, and the shortest correction. They cover declaration syntax, invalid types, duplicate effective names,
inheritance ambiguity, missing constructor fields, nullable assignments, explicit parsing, incompatible types, and
runtime shape mismatch.

Example:

```text
CompileError SCHEMA-E0304: Missing output field

Schema:
  OrderNormalized

Field:
  total

Problem:
  The output constructor does not provide a value for the target field.

Use:
  Add total=... to the constructor or copy it through OrderNormalized.base(source) when compatible.

See docs/reference/Schema.ref.md
```

Example:

```text
CompileError SCHEMA-E0302: Explicit conversion required

Output field:
  OrderNormalized.total: Decimal(12, 2), nullable=True

Source expression:
  order.total: String(), nullable=True

Use:
  total=to_decimal(order.total, precision=12, scale=2)

See docs/reference/Schema.ref.md
```

Example:

```text
Invalid schema field type:
  OrderRaw.id uses string

Use an explicit Structure type object:
  id = field(String(), nullable=False)

See docs/reference/Schema.ref.md
```

Example:

```text
Invalid decimal type:
  OrderNormalized.total uses Decimal(2, 12)

Decimal scale must be less than or equal to precision:
  total = field(Decimal(12, 2), nullable=True)

See docs/reference/Schema.ref.md
```

Example:

```text
Ambiguous inherited field:
  Order.id is declared by SourceKeys and BusinessKeys.

Resolve the field in Order:
  class Order(SourceKeys, BusinessKeys):
      id = field(String(), nullable=False, primary_key=True)

See docs/reference/Schema.ref.md
```

Example:

```text
Invalid schema base:
  Order inherits from SomePlainMixin, which is not a Schema class.

Use only Schema classes in schema inheritance.

See docs/reference/Schema.ref.md
```

## More Details

- [Schemas API](../api/Schemas.api.md) lists the compiler-visible declaration surface and PySpark parity.
- [Schema semantics background](../background/SchemaSemantics.back.md) records the original consolidated semantic
  contract.
- [Schema declaration syntax background](../background/SchemaDeclarationSyntax.back.md) contains source syntax detail.
- [Schema model background](../background/SchemaModel.back.md) contains compiler-model and Spark-mapping detail.
- [Schema inheritance background](../background/SchemaInheritance.back.md) contains worked composition examples.
- [Nullability and type coercion](../background/NullabilityAndTypeCoercion.back.md) defines expression assignment in
  detail.
- [Validation semantics](../background/ValidationSemantics.back.md) defines phase policy and runtime placement.
- [Data quality constraints](../background/DataQualityConstraints.back.md) defines the value-quality boundary.
