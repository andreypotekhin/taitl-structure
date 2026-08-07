# Schema

Schemas are Structure's typed row contracts. They drive compiler checks, generated Spark `StructType` values, online
execution, runtime validation, diagnostics, traceability, generated code, and generated documentation. A schema is a
declarative contract, not a data class, Python row object, raw PySpark `StructType`, primary-key declaration, or
uniqueness proof.

The [Schema reference](../reference/Schema.ref.md) is the end-user operation inventory. This background gathers the
semantic, model, inheritance, construction, nullability, validation, and data-quality contracts in reader order. The
normative source documents are [Schema Semantics](../dev/specifications/SchemaSemantics.spec.md),
[Schema Declaration Syntax](../dev/specifications/SchemaDeclarationSyntax.spec.md),
[Schema Model](../dev/specifications/SchemaModel.spec.md), [Schema Inheritance](../dev/specifications/SchemaInheritance.spec.md),
[Nullability and Type Coercion](../dev/specifications/NullabilityAndTypeCoercion.spec.md), and
[Data Quality Constraints](../dev/specifications/DataQualityConstraints.spec.md). The design sources are
[Schema Model](../dev/design/SchemaModel.design.md) and [Data Quality Constraints](../dev/design/DataQualityConstraints.design.md).

## Semantic Layers and Identity

Schema behavior has four layers:

1. Source declarations: Python classes inheriting `Schema` and declaring fields with supported hints, field factories,
   or both.
2. Compiler model: backend-neutral `SchemaDef`, `FieldDef`, and immutable type values.
3. Runtime shape: generated or execution-materialized Spark `StructType` values.
4. Value constraints: explicit data-quality rules outside the base shape model.

The schema model is the source of truth. Generated PySpark schemas and execution-materialized schemas are derived
artifacts and must not invent a different field order, type, nullability, or nested shape.

Each schema class is a nominal row-contract identity. Two classes can have identical fields and be structurally
compatible while remaining different schema identities. `SchemaDef.qualified_name` is the stable compiler identity;
source path and line are diagnostic metadata. Renaming a schema or moving it to another module changes its identity.
Generated schema constant names are derived deterministically from that identity and local naming rules.

Schema extraction, inheritance resolution, type validation, and compiler checks are Spark-free. They must not import
PySpark, start Java, create a `SparkSession`, inspect live data, or contact a Spark cluster.

## Canonical Declaration

The canonical PySpark schema form is:

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

The PySpark field DSL is imported from `structure.plugin.pyspark`. Standalone PySpark type values for casts and special
function contracts are available through `structure.plugin.pyspark.types`. Transform modules that need an expression
`array(...)` helper should import the PySpark DSL explicitly rather than combining wildcard imports.

Public schema examples should use the canonical form: import `Schema` from `structure` and field factories from
`structure.plugin.pyspark`. A schema declaration is intentionally ordinary Python class syntax so that it remains
cheap to inspect for compiler checks, generated schemas, runtime validation, traceability, and IDE navigation.

A field declaration has a Python class attribute name and a field factory, a Python hint, or both:

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

A bare supported hint infers the matching default factory. An explicit factory adds Spark-specific detail such as
nullability, aliases, decimal precision and scale, and collection-member nullability. Hints never control nullability;
the factory default and `nullable=` do.

### Schema Classes

A schema class inherits from `Schema` and declares fields with supported factories, hints, or both. Schema classes are
declarative contracts rather than data classes. Their constructors are used in transform methods to capture output
projections, not to create Python row objects.

Schema classes may inherit directly from `Schema` or from one or more user-defined schema classes. Every non-`object`
base must itself be a schema class, and Python must be able to construct a valid C3 MRO. User-defined non-field class
attributes are allowed when they do not look like failed field declarations. Public schema classes should be
import-safe.

### Declaration Grammar

The accepted declaration grammar is:

```text
schema_class      := class NAME(Schema): field_decl+
field_decl        := NAME ':' hint
                   | NAME ':' hint '=' field_factory(field_kwarg*)
                   | NAME '=' field_factory(field_kwarg*)
field_factory     := string() | integer() | long() | float() | double()
                   | boolean() | date() | timestamp()
                   | binary() | decimal(PRECISION, SCALE)
                   | array(type_expr, contains_null=BOOL?)
                   | struct(schema_ref)
                   | map(key_type, value_type, value_contains_null=BOOL?)
field_kwarg       := nullable=BOOL | alias=STRING | metadata=DICT | description=STRING
schema_ref        := Schema class object
```

Import-based discovery inspects runtime schema objects rather than parsing source text. Source text or AST inspection
may still supply diagnostics and source spans. An annotation without an assigned value declares a field; an annotated
ordinary assignment remains a class attribute and is not a field default.

### Python Hints

Bare hints infer these defaults:

```text
str                -> string()
bool               -> boolean()
int                -> integer()
float              -> double()
datetime.date      -> date()
datetime.datetime  -> timestamp()
list[T]            -> array(T)
dict[K, V]         -> map(K, V)
Schema subclass    -> struct(Schema subclass)
```

`decimal.Decimal` requires an explicit `decimal(precision, scale)` factory. When a factory is present, `int` also
accepts `long()`, `float` accepts `float()` or `double()`, and decimal fields require `Decimal`. Collection and struct
hints are checked recursively. `Optional`, unions, unparameterized collections, unsupported hints, and incompatible
hint/factory pairs are rejected.

## Field Semantics

Every field factory accepts the common options:

```python
string(
    nullable=True,
    alias=None,
    metadata=None,
    description=None,
)
```

The field attribute name is the Python name used by Structure source. `nullable` defaults to `True`. `alias` is the
physical Spark column name; when absent, the Python name is also the Spark name. `metadata` is an immutable mapping.
`description` is user-facing documentation carried into generated documentation, diagnostics, and traceability.

Field declaration order is class-body order after inheritance resolves. It is the order used for effective schemas,
generated Spark fields, symbolic projections, runtime validation, and generated documentation.

Structure does not declare primary keys or uniqueness. A required field is expressed only with `nullable=False` and does
not establish a key or a runtime quality rule.

Each effective field retains its name, type, nullability, alias, metadata, description, declaring schema, owning schema,
inherited and override status, and source location. These details are semantic model data for diagnostics, traceability,
and generated documentation; metadata and descriptions do not alter Spark shape semantics.

### Field Options in Use

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

`customer_id` is the Python name used in Structure code; `customer-id` is the Spark column read or written. Structure
does not declare primary keys or uniqueness through these options.

### Aliases

Use `alias=` when the physical Spark name differs from the Python name:

```python
class OrderRaw(Schema):
    id = string(nullable=False)
    promotion_code = string(nullable=True, alias="promo-code")
    customer_id = string(nullable=True, alias="customer id")
    class_ = string(nullable=True, alias="class")
    field_1st_code = string(nullable=True, alias="1st code")
```

Python expressions continue to use `order.promotion_code`; generated Spark schemas and projections use `promo-code`.
Structure passes aliases through unchanged. It does not sanitize, normalize, quote, or otherwise reinterpret backend
identifier edge cases.

Aliases are schema-local unless the field definition is inherited:

```python
class RawPromotion(Schema):
    promotion_code = string(nullable=True, alias="promo-code")


class NormalizedPromotion(Schema):
    promotion_code = string(nullable=True)


class StillRawPromotion(RawPromotion):
    pass
```

`RawPromotion.promotion_code` maps to `promo-code`, `NormalizedPromotion.promotion_code` maps to
`promotion_code`, and `StillRawPromotion.promotion_code` retains `promo-code`.

Aliases must be non-empty strings. Duplicate Python field names and duplicate effective Spark column names after
inheritance and alias resolution are rejected. Schema field `alias=` is separate from transform output `.alias(...)` and
generated Spark projection `.alias(...)`; those APIs name outputs or rendered columns and do not change schema fields.

For example, this duplicate effective Spark name is rejected:

```python
class Duplicate(Schema):
    promotion_code = string(alias="promo-code")
    alternate_code = string(alias="promo-code")
```

An invalid alias is rejected before compilation:

```python
string(alias="")
string(alias=123)
```

### Unsupported Declaration Syntax

The canonical syntax excludes:

- annotation-only fields when the hint is unsupported or unparameterized;
- dataclass-style defaults;
- Pydantic model inheritance as schema syntax;
- retired `field(...)` wrappers and standalone type-object declarations such as `String()`;
- implicit Spark type strings such as `"string"` or `"decimal(12,2)"`;
- raw PySpark fields;
- non-schema mixins.

Retired wrappers migrate mechanically:

```text
field(String(), nullable=False)        -> string(nullable=False)
field(Decimal(12, 2), nullable=True)   -> decimal(12, 2, nullable=True)
field(Array(String()))                  -> array(string())
field(Struct(Address))                  -> struct(Address)
```

Unknown field options are declaration errors. In particular, `primary_key=` is not a schema option.

## Type Model

Field factories return immutable Structure type values. Equality is structural:

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
BinaryType
ArrayType(item_type, contains_null)
StructType(schema)
MapType(key_type, value_type, value_contains_null)
```

### Scalar and Decimal Types

The scalar factories are `string()`, `integer()`, `long()`, `float()`, `double()`, `boolean()`, `date()`,
`timestamp()`, and `binary()`.

Their PySpark mapping is:

```text
string()     -> T.StringType()
integer()    -> T.IntegerType()
long()       -> T.LongType()
float()      -> T.FloatType()
double()     -> T.DoubleType()
boolean()    -> T.BooleanType()
date()       -> T.DateType()
timestamp()  -> T.TimestampType()
binary()     -> T.BinaryType()
```

`decimal(precision, scale)` requires positive integer precision and non-negative integer scale, with `scale <=
precision`. Omitted precision or scale is rejected. `decimal(12, 2)` maps to `T.DecimalType(12, 2)`.

One schema can use all scalar types; the choice is part of the contract and is never inferred from live data:

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

Decimal precision and scale are explicit. A currency field with ten integral digits and two fractional digits uses
`decimal(12, 2)`:

```python
class Payment(Schema):
    total = decimal(12, 2, nullable=False)
```

### Arrays

`array(item_type, contains_null=True)` declares an array. `item_type` must be a Structure type value; nested arrays
and arrays of structs are allowed. `contains_null` controls element nullability and defaults to `True`:

```text
array(string())                       -> T.ArrayType(T.StringType(), containsNull=True)
array(string(), contains_null=False)  -> T.ArrayType(T.StringType(), containsNull=False)
```

The array itself has its own field `nullable` setting. An absent array and a present array containing null elements are
separate contracts.

### Structs

`struct(schema_class)` declares a nested schema. The argument must be a `Schema` class, not an instance. The nested
field order is the effective inherited order of that class. Self-recursive schemas and cycles across multiple schemas
are rejected.

```python
class AddressBase(Schema):
    city = string(nullable=True)


class ShippingAddress(AddressBase):
    postal_code = string(nullable=True)


class Order(Schema):
    shipping = struct(ShippingAddress, nullable=True)
    previous_addresses = array(struct(ShippingAddress), nullable=True)
```

`struct(ShippingAddress)` maps to a `T.StructType` containing both `city` and `postal_code`. Struct type identity is
nominal: `struct(Address)` and `struct(BillingAddress)` are compatible only when they reference the same schema class.

### Maps

`map(key_type, value_type, value_contains_null=True)` declares maps. Both types must be Structure values, and the key
type must be a Spark-supported map-key type. Map keys are never nullable because Spark does not allow null map keys.
Nested map values, including struct values, are allowed. `value_contains_null` defaults to `True`:

```python
class Attribute(Schema):
    value = string(nullable=False)
    captured_at = timestamp(nullable=False)


class Product(Schema):
    attributes = map(string(), struct(Attribute), value_contains_null=False, nullable=True)
```

Higher-order array and map transformations are not implied by declaring collection fields and remain a separate
expression capability.

### Binary Values

`binary(nullable=...)` is a first-class data value. It participates in nested structs, arrays, maps, generated schemas,
validation, and ordinary projections. It is not a file or driver-side byte-processing abstraction.

The typed helpers are:

- `base64(value)`: Binary to nullable String;
- `unbase64(value)`: String to nullable Binary;
- `encode(value, charset="UTF-8")`: String to nullable Binary;
- `decode(value, charset="UTF-8")`: Binary to nullable String.

Charset names must be non-empty literal canonical names. Invalid base64 and malformed decoding remain capability-gated
until the PySpark target behavior agrees. The type mapper, expression checks, capability model, recipes, online
evaluator, renderer, explain output, and traceability must carry Binary explicitly. Generated code never logs binary
contents.

### Spark Type Mapping and Runtime Shape

The complete mapping includes:

```text
string()               -> T.StringType()
integer()              -> T.IntegerType()
long()                 -> T.LongType()
float()                -> T.FloatType()
double()               -> T.DoubleType()
decimal(12, 2)         -> T.DecimalType(12, 2)
boolean()              -> T.BooleanType()
date()                 -> T.DateType()
timestamp()            -> T.TimestampType()
binary()               -> T.BinaryType()
array(string())        -> T.ArrayType(T.StringType(), containsNull=True)
struct(Address)        -> T.StructType([...Address fields...])
map(string(), long())  -> T.MapType(T.StringType(), T.LongType(), valueContainsNull=True)
```

Schema generation is deterministic and uses effective field order. A field alias is the generated `StructField` name.
Generated schema constants are ordinary caller-facing `StructType` artifacts; they do not execute reads or writes.

## Schema Inheritance

Inheritance is ordered schema-to-schema field composition. It supports shared identifiers, audit fields, tenancy,
partition fields, and source metadata. It is not arbitrary Python mixin behavior.

### Canonical Form and Supported Bases

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

The effective order is `id`, `tenant_id`, `created_at`, `updated_at`, `customer_id`, then `total`.

A schema may inherit directly from `Schema` or from one or more user-defined schema classes. Every non-`object` base
must be a schema class, and Python must be able to construct a valid C3 MRO. Non-schema mixins are rejected.

Supported inheritance can be as small as one shared base:

```python
class Customer(EntityKeys):
    name = string(nullable=True)
```

and can combine multiple schema bases:

```python
class Order(EntityKeys, AuditFields):
    total = decimal(12, 2, nullable=True)
```

This is rejected because `SomePlainMixin` is not a schema class:

```python
class Order(EntityKeys, SomePlainMixin):
    total = decimal(12, 2, nullable=True)
```

### Effective Field Algorithm

The compiler builds an ordered effective field map:

1. Start with an empty ordered map.
2. Visit direct schema bases left to right as written in the class definition.
3. Recursively collect each base's effective fields before visiting the next base.
4. Skip a base already visited through a diamond path.
5. Add local fields in class-body declaration order.
6. Replace an inherited field in its existing position when a local field has the same name.
7. Append a new local field after all inherited fields.

Fields from the first base precede fields from the second base, and local fields follow inherited fields unless a local
override preserves an inherited position.

### Overrides and Conflicts

Overrides are whole-field replacements:

```python
class SoftDeleteFields(Schema):
    deleted_at = timestamp(nullable=True)


class RequiredDeleteMarker(SoftDeleteFields):
    deleted_at = timestamp(nullable=False)
```

The override keeps the inherited position. Its type, nullability, alias, metadata, and description replace the inherited
values; metadata and descriptions are not merged. Overriding with a non-field value and deleting an inherited field are
rejected.

Two unrelated bases defining the same field require a local resolution:

```python
class SourceKeys(Schema):
    id = string(nullable=False)


class BusinessKeys(Schema):
    id = string(nullable=False)


class Order(SourceKeys, BusinessKeys):
    id = string(nullable=False)
    total = decimal(12, 2, nullable=True)
```

The resolved field keeps the first inherited position. A shared diamond ancestor contributes only once.

Diamond inheritance is valid when the shared ancestor is reached through multiple paths:

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

`Keys.id` is collected once. By contrast, two unrelated bases that declare the same field require a local redeclaration;
the local field keeps the first inherited position.

### Field Origin and Identity

Every effective `FieldDef` retains final owning schema, declaring schema, field name, effective order, inherited status,
override status, overridden origin when applicable, and source location. This supports diagnostics, documentation,
traceability, and source navigation.

Inheritance does not erase nominal identity. `OrderRaw(EntityKeys)` and `CustomerRaw(EntityKeys)` can have compatible
structure while remaining different schema types. Transform flow validation uses schema identity unless a compatibility
rule explicitly requests structural compatibility.

Inheritance does not support deleting inherited fields, partial field overrides, metadata or description merging,
non-schema mixins, local reordering without a full redeclaration, or polymorphic transform dispatch based on schema
subclassing.

## Compiler Model

The model is extracted without PySpark:

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

`SchemaDef.fields` is the effective ordered list. `SchemaDef.local_fields` contains only declarations written directly
on the class. Python constructors, symbolic field access, diagnostics, and compiler checks use Python `name`; Spark
schemas, validation, expression rendering, and projection use `alias or name`.

`SchemaDef` represents one discovered `Schema` class: `name` is the class name, `qualified_name` is its importable
module-qualified identity, `module` is the source module, `source_path` and `source_line` are optional diagnostics,
`bases` lists direct schema bases in declaration order, `fields` is the effective output list, `local_fields` contains
direct declarations, and `constraints` and immutable `metadata` carry separate model information.

`FieldDef` represents one effective field. Its `name` is the Python attribute name; its `type`, `nullable`, `alias`,
immutable `metadata`, and optional `description` define the field contract. `declaring_schema` identifies where the
field was declared, `owning_schema` identifies the effective schema, `inherited` records whether it was inherited,
`overrides` records a replaced origin, and source path and line support diagnostics.

Extraction follows:

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

Extraction rejects invalid factory or hint declarations, invalid decimal bounds, invalid nested expressions, recursive
struct cycles, ambiguous inherited fields, non-schema bases, duplicate effective Python or Spark names, empty aliases,
and unsupported declaration shapes. Errors should link to the most specific schema specification.

Extraction should be cacheable by source fingerprint. Type values should be lightweight immutable objects; inheritance
resolution should be linear in schema classes plus field declarations; generated `StructType` text should be
deterministic
and cheap to emit. None of these operations may import PySpark or contact a Spark cluster.

The extraction pipeline is therefore: capture local fields, resolve inheritance, validate types, build `SchemaDef`, run
compile-time checks, generate the Spark schema, and validate runtime shape. Extraction should be cacheable by source
fingerprint, and type objects should remain lightweight immutable values.

## Symbolic Output Construction

Inside a compiled step method, calling a schema class creates a symbolic projection, not a Python row object:

```python
return OrderNormalized(
    id=order.id,
    customer_id=lower(trim(order.customer_id)),
    total=to_decimal(order.total, precision=12, scale=2),
)
```

Positional arguments, unknown keywords, and missing target fields fail during compilation. All non-nullable fields must
be
supplied or copied. Missing nullable fields are also errors in v1 so generated projections remain explicit and
reviewable.
Keyword order may differ from declaration order; generated projection order follows the target schema.

### Nested Struct Construction

Use the nested schema constructor for `struct(...)` fields:

```python
return OrderPublished(
    id=order.id,
    shipping=Address(
        city=trim(order.shipping.city),
        postal_code=order.shipping.postal_code,
    ),
)
```

Nested constructors lower to Spark `struct(...)` expressions, not Python objects or UDFs. Every nested field must be
assigned. The nested constructor must have the target schema identity. Partial nested updates, such as replacing only
`shipping.city`, are deferred; construct the whole nested value.

### Base Overlay Construction

For an output extending an earlier schema, `SchemaClass.base(...)` copies inherited fields and allows explicit overlays:

```python
return OrderWithCustomer.base(order)(
    customer_name=customer.name,
    customer_tier=customer.tier,
)
```

`base(...)` is symbolic construction syntax, not a nested field or runtime row object. It lowers to the same explicit
projection IR as a full constructor and generated PySpark remains an explicit `select(...)` in target field order.

Rules:

- `SchemaClass.base(source)(...)` copies inherited target fields by base-schema origin.
- Explicit overrides win over copied fields.
- Extra source fields are ignored.
- Unknown override keywords and missing target fields are errors.
- Copied fields must be type- and nullability-compatible unless explicitly overridden.
- `SchemaClass.base(source)` without the second call is valid only when every target field can be copied safely.
- One direct schema base takes one compatible source row.
- Multiple direct schema bases take one source row per direct base, in declaration order.
- A schema with no direct base cannot use `base(...)`; use `project(...)` for unrelated rows.
- The compiler maps by inherited field origin, not by searching all sources for matching names.
- Locally introduced and locally overridden fields must be supplied explicitly.

`base(...)` can be followed by `project(...)` when a child adds fields available by same-name compatible sources:

```python
return FulfillmentOption.base(demand).project(inventory)(
    available_to_promise=inventory.on_hand_quantity - inventory.reserved_quantity,
)
```

Combined construction copies inherited fields by base origin, fills still-unassigned fields by same-name projection,
then applies explicit keyword overrides. Explicit overrides always win.

Multiple direct bases use matching source rows:

```python
class OrderPublication(Schema):
    id = string(nullable=False)
    customer_name = string(nullable=True)
    total = decimal(12, 2, nullable=False)


class PublicationFlags(Schema):
    has_promotion = boolean(nullable=False)


class OrderPublished(OrderPublication, PublicationFlags):
    pass


flags = PublicationFlags(has_promotion=order.promotion_name.is_not_null())
return OrderPublished.base(order, flags)
```

Only fields required by each direct base are copied. Extra fields on `order` are ignored.

## Nullability and Assignment

Nullability is part of every field and every expression. Static nullability is conservative: it comes from declarations,
literals, helper rules, and simple filter narrowing. It does not scan data or prove arbitrary Python conditions.

A nullable expression cannot feed a non-nullable target unless it is narrowed or repaired. A
`where(parent_struct.is_not_null())` guard can narrow nested reads through that parent according to each nested field's
own declared nullability. Structure does not infer these facts from arbitrary predicates.

Assignment through joins follows the declared join semantics: a `left` join makes right-side fields nullable, while an
`inner` join preserves right-side declared nullability unless a later operation narrows it. Hooks do not establish
compile-time nullability facts unless a later hook postcondition contract says so.

### Spark SQL Assumptions

Structure records Spark SQL assumptions under `[tool.structure]`:

```toml
[tool.structure]
spark.sql.ansi.enabled = true
spark.sql.storeAssignmentPolicy = "ANSI"
```

Defaults are `spark.sql.ansi.enabled = true` and `spark.sql.storeAssignmentPolicy = "ANSI"`. The first setting must be
boolean. The second must be `ANSI`, `LEGACY`, or `STRICT`. These are compiler assumptions and generated-runtime
expectations; Structure does not start a Spark session or mutate its configuration.

The default ANSI policy accepts unsurprising widening and typed literals, requires visible parsing helpers, and rejects
lossy or policy-dependent conversions. Detailed `LEGACY` and `STRICT` checking may be deferred, but diagnostics must say
when the selected policy is not fully implemented.

### Fields, Literals, and Helpers

Field references inherit declared nullability. Python literals are valid Structure expressions:

- `None` is a nullable untyped null;
- `str` is non-null `string()`;
- `bool` is non-null `boolean()`;
- `int` is non-null `integer()`, or `long()` outside the 32-bit range;
- `float` is non-null `double()`;
- `datetime.date` is non-null `date()`;
- `datetime.datetime` is non-null `timestamp()`.

Most helpers are null-intolerant: nullable input produces a nullable result. `is_null(...)` and `is_not_null(...)`
return non-null booleans. `coalesce(...)` is non-null when at least one argument is statically non-null. A
`when(...).otherwise(...)` expression is non-null only when all result branches are statically non-null.

Basic row-local arithmetic `+`, `-`, and `*` is supported with conservative result typing based on the left operand
until fuller numeric formulas are specified.

### Filter Narrowing

`where(expr.is_not_null())` narrows direct field references after the filter in the same step method:

```python
def normalize(self, order: OrderRaw) -> OrderNormalized:
    where(order.id.is_not_null())
    return OrderNormalized(id=order.id)
```

Direct aliases may be narrowed when represented explicitly. Narrowing does not infer broad facts from arbitrary boolean
expressions, does not cross hooks, and may narrow nested reads only when the parent struct guard and nested field rules
make that fact explicit.

### Assignment Compatibility

In default ANSI mode, accepted assignments include exact matches, `None` to nullable fields, typed literals, integer to
long, float to double, compatible integer-to-decimal, and decimal widening that preserves integer digits and scale.

Rejected assignments include nullable to non-nullable, string to numeric/date/timestamp without a helper, numeric/date/
timestamp/boolean to string without an explicit conversion, double to float, lossy decimal narrowing, boolean/numeric
conversion, and incompatible array or struct members.

Decimal `decimal(p1, s1)` assigns to `decimal(p2, s2)` only when `s2 >= s1` and `p2 - s2 >= p1 - s1`. A 32-bit integer
needs at least ten integral decimal digits; a long needs nineteen. `coalesce(...)` uses a least-common-type rule where
untyped `0` may adopt the compatible decimal context:

```python
total=coalesce(to_decimal(order.total, precision=12, scale=2), 0)
```

Semantic parsing stays explicit:

```python
return OrderNormalized(
    total=to_decimal(order.total, precision=12, scale=2),
    ordered_on=to_date(order.ordered_on, format="yyyy-MM-dd"),
    processed_at=to_timestamp(order.processed_at),
)
```

Parsing helpers preserve input nullability unless their own semantics guarantee otherwise. Runtime parse failures follow
the helper and Spark configuration; a non-null target does not make parsing non-null.

## Runtime Shape and Validation

Generated `*_SCHEMA` constants and execution result schemas are equivalent shape-only Spark `StructType` artifacts. They
include effective Spark names, field order, data types, nullability, and nested shape.

```python
class Customer(Schema):
    id = string(nullable=False)
    name = string(nullable=True)
    tier = string(nullable=True)
```

```python
CUSTOMER_SCHEMA = T.StructType([
    T.StructField("id", T.StringType(), nullable=False),
    T.StructField("name", T.StringType(), nullable=True),
    T.StructField("tier", T.StringType(), nullable=True),
])
```

Callers may use generated schemas at storage boundaries:

```python
from structure_generated.store.pyspark.schemas.customer import *

customers = spark.read.schema(CUSTOMER_SCHEMA).parquet(customer_source_path)
```

Structure validates and projects DataFrames, but callers own reads, writes, table creation, partitioning, checkpoints,
output modes, and storage-specific options. Execution materializes equivalent schemas from `SchemaDef.fields` and
exposes them after `run(session)` without requiring generated files.

Generated schemas are shape-only artifacts. Callers may reuse them for their own reads, validation, or pre-write
projection, but generated schemas do not execute storage operations or value-level constraints.

### Validation Phases and Policy

Runtime validation has input, intermediate, and output phases:

```text
input -> intermediate -> output
```

Input validation checks supplied DataFrames. Intermediate validation checks each compiled step and attached hook at its
declared boundary. Output validation checks final returned frames.

The modes are `off`, `schema_only`, and `schema_and_constraints`. The default schema-first configuration is:

```toml
[tool.structure]
input_validation_mode = "schema_only"
validate_intermediate = true
intermediate_validation_mode = "schema_only"
output_validation_mode = "schema_only"
```

`validate_intermediate = false` is a compatibility shorthand for `intermediate_validation_mode = "off"`; an explicit
mode wins, and contradictory settings are configuration errors. Policy resolves from defaults through project and CLI
configuration, then transform, method, and hook-local overrides.

### Schema-Only Checks

Schema-only validation checks required and unexpected columns, strict projection order where configured, Spark data
types, reliable nullability metadata, nested struct shape, array element types, and map key/value types where available.
It never calls `count()`, `collect()`, `head()`, `toPandas()`, samples rows, aggregates, or otherwise scans data.

Input validation precedes the first step; step and hook validation occur at their configured boundaries; final
validation
precedes the result. Streaming DataFrames may use schema-only validation because it inspects metadata only. A hook may
allow extra columns with `SchemaMode.ALLOW_EXTRA_COLUMNS`; `project_output=True` restores target columns and order.
Generated and direct execution validate at identical boundaries.

## Data Quality Boundary

Schema shape and data quality are separate. Accepted values, ranges, patterns, decimal domains, uniqueness, referential
integrity, freshness, and row-count facts may require filters, limits, joins, aggregations, or actions. They must not be
silently enabled by a schema declaration.

`schema_and_constraints` is explicit opt-in for declared constraints at eligible phases:

```toml
[tool.structure]
input_validation_mode = "schema_and_constraints"
intermediate_validation_mode = "schema_and_constraints"
output_validation_mode = "schema_and_constraints"
```

Until concrete constraint families are available, the mode may report that only schema checks exist. Generated schemas
remain shape-only; future constraint metadata must use separate artifacts unless a later design changes this contract.

Potential field-local families are accepted values, numeric and temporal ranges, patterns, length limits, and decimal
domains. Schema-level families are unique keys, composite unique keys, cross-field conditions, row-count bounds, and
freshness. Cross-dataset families are referential and anti-existence checks.

Each constraint declares its target, phases, kind, severity, source location, and cost class: compile-time only,
schema-only, row-local, aggregation-based, or join-based. Cost controls default eligibility and streaming support.
Schema-only validation is streaming-compatible. Aggregation, join, row-count, and freshness checks are batch-only unless
a narrower streaming rule proves otherwise.

## Schema-Carrying JSON and CSV

`from_json(value, as_=Schema, options=...)` and `from_csv(value, as_=Schema, options=...)` return the exact declared
struct shape. `to_json(value, options=...)` and `to_csv(value, options=...)` accept typed struct values and return
nullable String. Parser schemas are never inferred; on supported permissive PySpark profiles, parsed fields are
nullable, so parser schemas must declare each parsed field nullable.

`JsonOptions` and `CsvOptions` are immutable literal-only records. The initial options are delimiter, quote, escape,
null value, date format, timestamp format, and permissive parse mode. Dictionaries, Columns, callbacks, unknown keys,
dynamic option values, map/variant results, schema inference, file options, and streaming claims are outside this
contract.

Malformed input follows the verified nullable behavior of the selected target. Disagreement is a capability diagnostic,
not an unannounced execution difference. JSON and CSV lower through shared recipes: JSON uses a generated `StructType`
schema argument, while CSV uses the target-compatible DDL schema form. Tests cover options, nested records, nullability,
malformed values, generated source, online/generated parity, diagnostics, and supported live PySpark behavior.

## Diagnostics and Source Boundaries

Schema diagnostics identify schema and field, phase when relevant, source expression or DataFrame column, expected and
actual type/nullability, source location, problem, shortest correction, and a link to the narrowest specification.

Common failures include unknown field options, invalid hints or types, invalid decimal bounds, recursive structs,
ambiguous inheritance, duplicate Python or Spark names, invalid aliases, missing output fields, incompatible assignment,
nullable-to-non-nullable assignment, missing explicit parsing, and runtime shape mismatch.

```text
CompileError SCHEMA-E0304: Missing output field

Schema:
  OrderNormalized

Field:
  total

Use:
  Add total=... or copy it through OrderNormalized.base(source) when compatible.

See docs/dev/specifications/SchemaSemantics.spec.md
```

```text
CompileError SCHEMA-E0302: Explicit conversion required

Output field:
  OrderNormalized.total: decimal(12, 2), nullable=True

Source expression:
  order.total: string(), nullable=True

Use:
  total=to_decimal(order.total, precision=12, scale=2)

See docs/dev/specifications/NullabilityAndTypeCoercion.spec.md
```

Inheritance diagnostics include schema, conflicting field, involved bases, source location, and a local redeclaration
fix. Runtime constraint diagnostics additionally include constraint kind and cost because action-triggering checks are
materially different from schema-shape failures.

Additional declaration diagnostics should remain actionable:

```text
Invalid schema field type:
  OrderRaw.id uses string

Use an explicit Structure type object:
  id = string(nullable=False)

See docs/dev/specifications/SchemaDeclarationSyntax.spec.md
```

```text
Invalid decimal type:
  OrderNormalized.total uses decimal(2, 12)

Decimal scale must be less than or equal to precision:
  total = decimal(12, 2, nullable=True)

See docs/dev/specifications/SchemaModel.spec.md
```

```text
Ambiguous inherited field:
  Order.id is declared by SourceKeys and BusinessKeys.

Resolve the field in Order with a local declaration:
  id = string(nullable=False)

See docs/dev/specifications/SchemaInheritance.spec.md
```

```text
Invalid schema base:
  Order inherits from SomePlainMixin, which is not a Schema class.

Use only Schema classes in schema inheritance.

See docs/dev/specifications/SchemaInheritance.spec.md
```

## Implementation and Acceptance Contract

The implementation must:

1. represent immutable scalar, decimal, array, map, struct, and binary types;
2. capture local field order and direct schema bases;
3. resolve effective inheritance with diamond deduplication and conflict detection;
4. retain field origin, override, alias, metadata, description, and source location;
5. validate hints, factories, decimal bounds, nested types, aliases, and recursive cycles;
6. build `SchemaDef` and `FieldDef` without PySpark;
7. construct symbolic outputs, nested structs, base overlays, and explicit projections;
8. propagate type and nullability and check assignment compatibility;
9. generate deterministic Spark schemas from `SchemaDef.fields`;
10. materialize equivalent schemas during execution;
11. keep generated schema constants shape-only;
12. keep schema-only validation free of Spark actions;
13. classify future constraints by phase, cost, and streaming compatibility;
14. provide diagnostics linked to the most specific source document.

Acceptance includes primitive, decimal, array, map, struct, binary, hinted, aliased, inherited, overridden, diamond,
and conflicting schemas; deterministic generated fields; caller reuse of generated `StructType`; equivalent execution
schemas; constructor and base-overlay failures; nullability narrowing; explicit parsing; schema-only validation without
row scans; and actionable diagnostics.

## Deliberate Non-Goals

Schema declarations do not provide Python row objects, primary-key enforcement, uniqueness proofs, implicit data scans,
hidden semantic casts, arbitrary Python mixin composition, partial nested updates, polymorphic transform dispatch based
on schema subclassing, storage orchestration, checkpoints, writes, or automatic value-level constraint execution.

## More Details

- [Schemas API](../api/Schemas.api.md) lists the compiler-visible declaration surface and PySpark parity.
- [Schema reference](../reference/Schema.ref.md) lists the end-user operation surface summarized by this background.
