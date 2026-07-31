# Schema Reference

Schemas define Structure's typed row contracts. They drive compiler checks, generated Spark schemas, execution,
runtime validation, diagnostics, traceability, generated code, and generated documentation. A schema is a declarative
contract, not a data class, a Python row object, or a raw PySpark `StructType`.

The schema model is the source of truth. Generated PySpark schemas and execution-materialized schemas are derived
artifacts.

## Semantic Contract

Structure schema behavior has four layers:

1. Source declarations: Python classes that inherit `Schema` and declare fields with field factories.
2. Compiler model: backend-neutral `SchemaDef`, `FieldDef`, and type values.
3. Runtime shape: generated or materialized Spark `StructType` values.
4. Value constraints: explicit data-quality rules outside the base shape model.

Schema extraction, type validation, inheritance resolution, and compiler checks are Spark-free. They must not import
PySpark, start Java, create a `SparkSession`, or inspect live data.

### Canonical Declaration

The canonical declaration form is explicit:

```python
from structure import Schema
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

Rules:

- Every field uses its matching field factory, such as `string(...)` or `decimal(12, 2, ...)`.
- Field factories create immutable Structure type contracts.
- Field order is class-body order after inheritance is resolved.
- Field names are Python attribute names.
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

The PySpark schema declaration DSL is imported from `structure.plugin.pyspark`:

```python
from structure import Schema
from structure.plugin.pyspark import *
```

Standalone PySpark type values for casts and UDF contracts are available through
`structure.plugin.pyspark.types`.

### Grammar

In descriptive form, the accepted schema declaration grammar is:

```text
schema_class      := class NAME(Schema): field_decl*
field_decl        := NAME ':' hint | NAME ':' hint '=' field_factory(field_kwarg*) | NAME '=' field_factory(field_kwarg*)
field_factory     := scalar_type | decimal_type | array_type | struct_type | map_type
scalar_type       := string() | integer() | long() | float() | double() | boolean() | date() | timestamp()
decimal_type      := decimal(precision, scale)
array_type        := array(type_expr, contains_null=BOOL?)
struct_type       := struct(schema_ref)
map_type          := map(key_type, value_type, value_contains_null=BOOL?)
field_kwarg       := nullable=BOOL | alias=STRING | metadata=DICT | description=STRING
schema_ref        := Schema class object
```

The compiler should implement this grammar by inspecting runtime schema objects when import-based discovery is used.
Source text or AST inspection may still be used for diagnostics and source spans.

Python hints are schema declarations. A bare `str`, `bool`, `int`, `float`,
`datetime.date`, `datetime.datetime`, `list[T]`, `dict[K, V]`, or Schema
subclass infers the matching default PySpark field. `float` infers `double()`;
`Decimal` requires an explicit `decimal(precision, scale)` factory. A factory
may add Spark-specific detail and is validated against the hint. Hints do not
control nullability: factory defaults and `nullable=` do. Optional/union and
unparameterized collection hints are invalid. An annotated ordinary assignment
is still a class attribute, not a field default.

Raw PySpark fields, implicit Spark type strings, and non-schema mixins are
outside the canonical form.

### Schema Classes

A schema class is a class inheriting from `Schema` with field-factory attributes.

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

Each field factory accepts `nullable=`, `alias=`, `metadata=`, and `description=`. For example:

```python
total = decimal(12, 2, nullable=False, description="Order total after discounts.")
```

`nullable` defaults to `True`. `alias` is the Spark column name; otherwise the Python attribute name is also the Spark
name.
`metadata` is an immutable mapping. `description` feeds generated documentation, diagnostics, and traceability.

Each effective field records:

```text
name
type
nullable
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

### Field Options In Use

This declaration uses every field option in its ordinary role:

```python
class CustomerSource(Schema):
    customer_id = string(nullable=False, alias='customer-id', metadata={'source': 'crm', 'pii': 'indirect'}, description='Stable customer identifier supplied by the CRM.')
    display_name = string(nullable=True, description='Name displayed to account users.')
```

`customer_id` is the Python name used in Structure code; `customer-id` is the column that Spark reads and writes.
Structure does not declare primary keys or uniqueness. `metadata` and `description` travel with the compiler model for
generated documentation, diagnostics, and traceability; neither changes the Spark type or validates data values.

### Field Aliases

Use `alias=...` when the physical Spark column name is not the Python field name. This is common for source columns
with spaces, hyphens, reserved words, leading digits, or legacy names that should not leak into Python code.

```python
class OrderRaw(Schema):
    id = string(nullable=False)
    promotion_code = string(nullable=True, alias='promo-code')
    customer_id = string(nullable=True, alias='customer id')
    class_ = string(nullable=True, alias='class')
    field_1st_code = string(nullable=True, alias='1st code')
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
    promotion_code = string(nullable=True, alias='promo-code')


class NormalizedPromotion(Schema):
    promotion_code = string(nullable=True)


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
string(alias='')    # rejected: empty alias
string(alias=123)   # rejected: alias is not a string
```

Duplicate effective Spark column names are rejected after aliases and inheritance are resolved:

```python
class Duplicate(Schema):
    promotion_code = string(alias='promo-code')
    alternate_code = string(alias='promo-code')  # rejected
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

That generated Spark alias is an implementation detail of projection rendering. In schema declarations, use the
factory option, such as `string(alias="spark-column-name")`.

### Unsupported Syntax

The following are not part of the canonical syntax:

- annotation-only field declarations such as `id: String`;
- dataclass-style defaults;
- Pydantic model inheritance as schema syntax;
- `field(...)` wrappers and standalone type-object declarations such as `String()`;
- implicit Spark type strings such as `"string"` or `"decimal(12,2)"`;
- non-schema mixins.

Retired `field(...)` declarations migrate mechanically to factories:

```text
field(String(), nullable=False)        -> string(nullable=False)
field(Decimal(12, 2), nullable=True)   -> decimal(12, 2, nullable=True)
field(Array(String()))                  -> array(string())
field(Struct(Address))                  -> struct(Address)
```

`field(...)` is rejected. Docs, fixtures, and generated examples use only field factories.

## Types

All schema type constructors return immutable value objects. Equality is structural.

```text
string() == string()
decimal(12, 2) == decimal(12, 2)
array(string()) == array(string())
struct(Address) == struct(Address)
```

The type model is:

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

The scalar field factories are:

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

One schema can use all scalar types. The choice is part of the contract; Structure does not infer a field type from
live data:

```python
class ScalarSample(Schema):
    label = string(nullable=False)
    item_count = integer(nullable=False)
    event_sequence = long(nullable=False)
    confidence = float(nullable=True)
    score = double(nullable=True)
    active = boolean(nullable=False)
    business_date = date(nullable=False)
    recorded_at = timestamp(nullable=False)
```

### Decimal

`decimal(precision, scale)` requires an integer precision of at least one and a scale from zero through that precision.

Rules:

- `precision >= 1`
- `scale >= 0`
- `scale <= precision`
- Omitted precision or scale is rejected.

Generated PySpark mapping:

```text
decimal(12, 2) -> T.DecimalType(12, 2)
```

For example, a currency field with up to ten digits before the decimal point and two after it is declared as:

```python
class Payment(Schema):
    total = decimal(12, 2, nullable=False)
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

For example, `tags` may be absent as a whole, while a present tag list may not contain null elements:

```python
class TaggedOrder(Schema):
    tags = array(string(), contains_null=False, nullable=True)
```

### Struct

`struct(schema)` declares a nested schema.

Rules:

- `schema` must be a `Schema` class, not an instance.
- `struct(Address)` identifies a particular schema class.
- `struct(Address)` and `struct(BillingAddress)` are compatible only when they reference the same schema class.
- Nested struct field order follows the referenced schema class.
- `struct(SchemaClass)` uses the effective inherited field set of `SchemaClass`.
- Self-recursive schemas and recursive cycles across multiple schemas are rejected.

Example:

```python
class AddressBase(Schema):
    city = string(nullable=True)


class ShippingAddress(AddressBase):
    postal_code = string(nullable=True)


class Order(Schema):
    shipping = struct(ShippingAddress, nullable=True)
    previous_addresses = array(struct(ShippingAddress), nullable=True)
```

Generated Spark schema for both fields includes `city` and `postal_code`; `previous_addresses` is an array whose
elements use that same nested shape.

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
- Higher-order map transformations remain a v2 expression feature.

Generated PySpark mapping:

```text
map(string(), string())  -> T.MapType(T.StringType(), T.StringType(), valueContainsNull=True)
```

For example, a sparse set of typed attributes can use struct values and require every present value to be non-null:

```python
class Attribute(Schema):
    value = string(nullable=False)
    captured_at = timestamp(nullable=False)


class Product(Schema):
    attributes = map(string(), struct(Attribute), value_contains_null=False, nullable=True)
```

### Spark Type Mapping

Full mapping:

```text
string()              -> T.StringType()
integer()             -> T.IntegerType()
long()                -> T.LongType()
float()               -> T.FloatType()
double()              -> T.DoubleType()
decimal(12, 2)        -> T.DecimalType(12, 2)
boolean()             -> T.BooleanType()
date()                -> T.DateType()
timestamp()           -> T.TimestampType()
array(string())       -> T.ArrayType(T.StringType(), containsNull=True)
struct(Address)       -> T.StructType([...])
map(string(), long()) -> T.MapType(T.StringType(), T.LongType(), valueContainsNull=True)
```

Spark schema generation must be deterministic and formatted consistently. When a field has an alias, Spark schema
generation uses the alias as the `StructField` name.

## Inheritance

Schema inheritance is ordered schema-to-schema field composition. It is especially useful for shared identifiers, audit
columns, partition columns, tenancy fields, and common source metadata. It is not arbitrary Python mixin behavior.

### Canonical Form

```python
from structure import Schema
from structure.plugin.pyspark import *


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
    name = string(nullable=True)
```

```python
class Order(EntityKeys, AuditFields):
    total = decimal(12, 2, nullable=True)
```

Non-schema mixins are not supported:

```python
class Order(EntityKeys, SomePlainMixin):  # rejected
    total = decimal(12, 2, nullable=True)
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
    deleted_at = timestamp(nullable=True)


class RequiredDeleteMarker(SoftDeleteFields):
    deleted_at = timestamp(nullable=False)
```

Override rules:

- Override replacement is whole-field replacement.
- Override position is the inherited field position.
- Type, nullability, alias, metadata, and description all come from the overriding
- Metadata is not merged.
- Description is not merged.
- Overriding a field with a non-field value is rejected.
- Deleting an inherited field is not supported.

Whole-field replacement keeps behavior visible. A reader can inspect the overriding line and know the final field
definition.

### Duplicate Fields Across Bases

If two unrelated bases define the same field name, the subclass must redeclare that field locally to resolve the
ambiguity.

Rejected:

```python
class SourceKeys(Schema):
    id = string(nullable=False)


class BusinessKeys(Schema):
    id = string(nullable=False)


class Order(SourceKeys, BusinessKeys):
    total = decimal(12, 2, nullable=True)
```

Accepted:

```python
class Order(SourceKeys, BusinessKeys):
    id = string(nullable=False)
    total = decimal(12, 2, nullable=True)
```

The resolved `id` keeps the first inherited position. In the accepted example, `id` remains before `total`.

Diamond inheritance through a shared base is not a duplicate:

```python
class Keys(Schema):
    id = string(nullable=False)


class CustomerKeys(Keys):
    customer_id = string(nullable=False)


class ProductKeys(Keys):
    product_id = string(nullable=False)


class CustomerProduct(CustomerKeys, ProductKeys):
    score = decimal(8, 4, nullable=True)
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

The following are not supported:

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

`FieldDef` represents one effective schema

Rules:

- `name` is the Python class attribute name.
- `type` is a Structure `TypeDef`.
- `nullable` defaults to `True`.
- `alias` is an optional Spark column name. If absent, the Spark column name is `name`.
- `metadata` is immutable and defaults to empty.
- `description` is optional.
- `declaring_schema` is the schema class that declared the effective
- `owning_schema` is the schema whose effective field list contains this
- `inherited` is true when `declaring_schema != owning_schema`.
- `overrides` points to the overridden field origin when the field replaces an inherited

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

- invalid or non-field-factory declarations;
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
- Missing nullable fields are errors. Developers should be explicit to keep generated projections reviewable.
- All non-nullable output fields must be supplied unless defaults are introduced by a later spec.
- Field keyword order may differ from declaration order.
- Generated projection order follows target schema declaration order, not source keyword order.
- Assignment type and nullability are checked before generated or direct runtime execution.

### Nested Struct Construction

For nested `struct(...)` fields, use the nested schema constructor as the assigned value:

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
- The constructed schema must match the target `struct(...)` schema identity.
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
- `base(...)` is invalid for a schema with no direct schema base; use `project(...)` for unrelated rows.
- The compiler maps fields by inherited field origin, not by searching all sources for a matching field name.
- Fields introduced locally by the target schema must be supplied as explicit overrides unless they can be copied by a
  later spec-defined default.
- Fields locally overriding inherited fields must be supplied explicitly; this keeps changed type, nullability,
  metadata, or meaning visible at the construction site.

`base(...)` can be followed by `project(...)` when a child schema adds fields that already exist by name on another
source row:

```python
return FulfillmentOption.base(demand).project(inventory)(
    available_to_promise=inventory.on_hand_quantity - inventory.reserved_quantity,
)
```

Combined construction applies sources in this order:

- `base(...)` copies inherited target fields by base-schema origin.
- `project(...)` fills still-unassigned target fields by same-name compatible source fields.
- Explicit keyword overrides win over both copied and projected fields.

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
`PublicationFlags` are copied from `flags`. The `order` row may have extra fields from earlier enrichment stages; only
fields required by `OrderPublication` are copied.

## Nullability And Assignment

Nullability is part of every field and every expression.

Rules:

- A nullable expression cannot feed a non-nullable target unless narrowed or repaired.
- `where(expr.is_not_null())` narrows simple field references after the filter in the same step method.
- `where(parent_struct.is_not_null())` narrows nested reads through that parent according to each nested field's own
  declared nullability.
- `"left"` makes joined right-side fields nullable after the join.
- `"inner"` preserves right-side declared nullability unless later operations narrow it.
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

`decimal(p1, s1)` assigns to `decimal(p2, s2)` only if `s2 >= s1` and `p2 - s2 >= p1 - s1`. A 32-bit integer needs at
least ten integral decimal digits; a long needs nineteen. `coalesce` computes a least common type, so an untyped `0`
can become `decimal(12, 2)` when its other argument and output target establish that context.

Use explicit semantic parsing conversions for string data:

```python
return OrderNormalized(
    total=to_decimal(order.total, precision=12, scale=2),
    ordered_on=to_date(order.ordered_on, format="yyyy-MM-dd"),
    processed_at=to_timestamp(order.processed_at),
)
```

Ordinary compatible assignments stay compact. Here an integer field widens to `long()`, `None` supplies a nullable
field, and parsing remains explicit where the source and target have different meaning:

```python
class OrderRaw(Schema):
    item_count = integer(nullable=False)
    total = string(nullable=True)


class OrderNormalized(Schema):
    item_count = long(nullable=False)
    total = decimal(12, 2, nullable=True)
    rejection_reason = string(nullable=True)


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
    id = string(nullable=False)
    name = string(nullable=True)
    tier = string(nullable=True)
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
from structure_generated.store.pyspark.schemas.customer import CUSTOMER_SCHEMA

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
  The output constructor does not provide a value for the target

Use:
  Add total=... to the constructor or copy it through OrderNormalized.base(source) when compatible.

See docs/reference/Schema.ref.md
```

Example:

```text
CompileError SCHEMA-E0302: Explicit conversion required

Output field:
  OrderNormalized.total: decimal(12, 2), nullable=True

Source expression:
  order.total: string(), nullable=True

Use:
  total=to_decimal(order.total, precision=12, scale=2)

See docs/reference/Schema.ref.md
```

Example:

```text
Invalid schema field type:
  OrderRaw.id uses string

Use an explicit Structure type object:
  id = string(nullable=False)

See docs/reference/Schema.ref.md
```

Example:

```text
Invalid decimal type:
  OrderNormalized.total uses decimal(2, 12)

Decimal scale must be less than or equal to precision:
  total = decimal(12, 2, nullable=True)

See docs/reference/Schema.ref.md
```

Example:

```text
Ambiguous inherited field:
  Order.id is declared by SourceKeys and BusinessKeys.

Resolve the field in Order:
  class Order(SourceKeys, BusinessKeys):
      id = string(nullable=False)

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
