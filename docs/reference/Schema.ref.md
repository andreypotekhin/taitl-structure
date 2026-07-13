# Schema Reference

Structure schemas define row shape and type meaning for compiler checks, generated Spark schemas, runtime validation,
online execution, generated code, diagnostics, traceability, and generated documentation. A schema is a declarative
contract, not a data class, a Python row object, or a raw PySpark `StructType`.

The schema model is the source of truth. Generated PySpark schemas and online materialized schemas are derived
artifacts. See the [Schemas API](../api/Schemas.api.md) for the complete public declaration surface and PySpark parity.

## Semantic Layers

Structure schema behavior has four layers:

1. Source declarations: Python classes that inherit `Schema` and declare fields with `field(...)`.
2. Compiler model: backend-neutral schema, field, type, source-location, and inheritance metadata.
3. Runtime shape: generated or materialized Spark `StructType` values.
4. Value constraints: explicit data-quality rules outside the base shape model.

Schema extraction, type validation, inheritance resolution, and compiler checks are Spark-free. They must not import
PySpark, start Java, create a `SparkSession`, or inspect live data.

## Declaration Syntax

The canonical declaration form is explicit:

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

In descriptive form, the accepted grammar is:

```text
schema_class      := class NAME(Schema): field_decl+
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

Lowercase type sentinels, annotation-only declarations, dataclass-style defaults, raw PySpark fields, implicit Spark
type strings, and non-schema mixins are outside the canonical form.

## Fields

`field(...)` has this shape:

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
Metadata is an immutable mapping and descriptions feed generated documentation, diagnostics, and traceability.

Field declaration order is class-body order after inheritance resolves. It is the order of generated fields,
projections, runtime validation, and documentation. Python field names and effective Spark column names must each be
unique. Aliases are schema-local except when the field itself is inherited. Structure passes aliases to Spark unchanged;
it does not sanitize, normalize, or quote backend-specific identifiers.

## Schema Model And Identity

A discovered schema has a name, qualified name, module, source location, direct bases, effective fields, local fields,
constraints, and immutable metadata. Each effective field records its name, type, nullability, key flag, alias,
metadata, description, declaring schema, owning schema, inheritance status, override origin, and source location.

The qualified class name is the stable schema identity. Two schemas with identical fields may be structurally
compatible, but they are distinct named contracts. Renaming a class or moving it between modules changes its identity,
generated constant names, traceability, and diagnostics. Source paths and lines are diagnostic metadata, not identity.

## Types

All type values are immutable and structurally comparable. The scalar types are:

```text
String()  Integer()  Long()  Float()  Double()  Boolean()  Date()  Timestamp()
Decimal(precision, scale)
```

`Decimal` requires an integer precision of at least one and a scale from zero through that precision. `Array` records
its element type and `contains_null`; nested arrays and arrays of structs are supported. `Map` records key type, value
type, and value nullability; map keys cannot be null. `Struct(SchemaClass)` identifies a particular schema class, so
two same-shaped schemas remain distinct nested types. Self-recursive structs and recursive cycles are rejected.

Generated PySpark mappings are direct: `String()` becomes `T.StringType()`, `Decimal(12, 2)` becomes
`T.DecimalType(12, 2)`, `Array(String(), contains_null=False)` becomes a non-null-element `T.ArrayType`, and `Struct`
expands to the referenced schema's effective `T.StructType`.

## Inheritance

Schema inheritance is ordered field composition. A schema may inherit directly from `Schema` or from one or more
user-defined schema classes. All non-`object` bases must be schemas, be import-safe, and form a valid Python C3 MRO.

The compiler builds the effective field map as follows:

1. Visit direct schema bases from left to right.
2. Recursively collect each base's effective fields before later bases.
3. Collect a shared diamond ancestor once.
4. Add local fields in declaration order.
5. Replace an inherited field in place when a local field has the same name.
6. Append new local fields after inherited fields.

An override replaces the whole field: type, nullability, primary-key flag, metadata, and description. Metadata and
descriptions do not merge. Unrelated bases that provide the same field require a local redeclaration to resolve the
ambiguity. Deleting inherited fields, partial overrides, local reordering, and polymorphic dispatch by schema subclass
are unsupported.

### Canonical Form

```python
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

Its effective field order is `id`, `tenant_id`, `created_at`, `updated_at`, `customer_id`, then `total`. This is the
generated schema, projection, and strict-validation order.

### Overrides

```python
class SoftDeleteFields(Schema):
    deleted_at = field(Timestamp(), nullable=True)


class RequiredDeleteMarker(SoftDeleteFields):
    deleted_at = field(Timestamp(), nullable=False)
```

The replacement stays in the inherited position. It replaces the entire field; no metadata or description is merged.

### Duplicate Fields And Diamonds

Two unrelated bases must be resolved locally:

```python
class SourceKeys(Schema):
    id = field(String(), nullable=False)


class BusinessKeys(Schema):
    id = field(String(), nullable=False, primary_key=True)


class Order(SourceKeys, BusinessKeys):
    id = field(String(), nullable=False, primary_key=True)
    total = field(Decimal(12, 2), nullable=True)
```

The resolved `id` keeps the first inherited position. A shared diamond ancestor is collected once:

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

`CustomerProduct` has one `id`, followed by `customer_id`, `product_id`, and `score`. For every effective field,
Structure retains owner, declarer, effective order, inherited status, override status, and overridden origin for
diagnostics, traceability, documentation, and source navigation.

### Nested Schemas

`Struct(SchemaClass)` uses the effective inherited fields of its schema:

```python
class AddressBase(Schema):
    city = field(String(), nullable=True)


class ShippingAddress(AddressBase):
    postal_code = field(String(), nullable=True)


class Order(Schema):
    shipping = field(Struct(ShippingAddress), nullable=True)
```

The Spark schema for `shipping` contains `city` and `postal_code`.

## Symbolic Construction

Calling a schema in a compiled transform creates a typed symbolic projection. It does not create a Python record or run
row-by-row Python code. Constructors accept keyword values only. Unknown fields, positional arguments, and omitted
target fields are errors; projection order follows the target schema rather than keyword order.

```python
return OrderNormalized(
    id=order.id,
    customer_id=lower(trim(order.customer_id)),
    total=to_decimal(order.total, precision=12, scale=2),
)
```

Nested values must be copied as a compatible whole struct or built with the nested schema constructor. A nested
constructor supplies every nested field and lowers to Spark `struct(...)`; partial nested mutation is not supported.

`SchemaClass.base(...)(...)` copies compatible inherited target fields from one or more source rows, then applies
explicit overrides. Extra source fields are ignored. Missing target fields and unknown overrides are errors. With one
direct schema base, `base(...)` accepts its compatible source. With multiple bases, it accepts one source per direct
base in left-to-right declaration order. Local target fields and locally overridden inherited fields must be explicit.

### Complete And Nested Constructors

Every target field, including nullable fields, must be explicit or safely copied; Structure does not insert missing
nullable values. A nested constructor supplies every nested field and lowers to Spark `struct(...)`:

```python
return OrderPublished(
    id=order.id,
    shipping=Address(
        city=trim(order.shipping.city),
        postal_code=order.shipping.postal_code,
    ),
)
```

Partial nested updates are not supported. Construct or copy the whole nested value.

### Base Overlay Examples

Base overlays expand to the same explicit projection as a full constructor:

```python
return OrderWithCustomer.base(order)(
    customer_name=customer.name,
    customer_tier=customer.tier,
)
```

Explicit overrides win. Extra source fields are ignored. `SchemaClass.base(source)` without overrides is valid only when
every target field can be copied safely. For several direct bases, source arguments follow base declaration order:

```python
class OrderPublication(Schema):
    id = field(String(), nullable=False, primary_key=True)
    customer_name = field(String(), nullable=True)


class PublicationFlags(Schema):
    has_promotion = field(Boolean(), nullable=False)


class OrderPublished(OrderPublication, PublicationFlags):
    pass


flags = PublicationFlags(has_promotion=order.promotion_name.is_not_null())
return OrderPublished.base(order, flags)
```

Fields are selected by inheritance origin, not by an opportunistic same-name source-field search.

## Nullability And Assignment

Every field and expression has conservative static nullability. Field references inherit their declared nullability;
non-null Python literals are non-null, while `None` is an untyped nullable literal. Most expression helpers propagate
nullable inputs. `is_null` and `is_not_null` are non-null booleans; `coalesce` is non-null when any argument is known
non-null; a `when(...).otherwise(...)` expression is non-null only when every result branch is non-null.

`where(field.is_not_null())` narrows a direct field reference after that call in the same step method. It does not infer
facts from arbitrary predicates or cross hook boundaries. A left join makes right-side fields nullable; an inner join
preserves their declared nullability unless a later operation narrows it.

An output assignment requires compatible type and nullability. Under the default ANSI policy, exact matches, safe
numeric widening, typed literals, and compatible decimal widening are accepted. String parsing and numeric-to-string
conversion, lossy decimal narrowing, double-to-float, numeric-to-boolean, boolean-to-numeric, and incompatible values
require an explicit conversion or fail.

`Decimal(p1, s1)` assigns to `Decimal(p2, s2)` only when `s2 >= s1` and `p2 - s2 >= p1 - s1`. Semantic parsing must be
visible through helpers such as `to_decimal`, `to_date`, and `to_timestamp`; Structure never silently treats parsing as
a harmless assignment. `coalesce` and similar multi-value helpers compute a least common Structure type and may use an
output target to type an otherwise untyped literal.

The compiler assumes `spark.sql.ansi.enabled = true` and `spark.sql.storeAssignmentPolicy = "ANSI"` by default. It
records but does not silently mutate those Spark settings. `STRICT` and `LEGACY` configurations require the checker to
either explain their behavior or reject non-exact assignment clearly.

### Narrowing And Repair

Most helpers, including `lower`, `trim`, and `to_decimal`, propagate nullable input. `is_null` and `is_not_null` return
non-null booleans. `coalesce` is non-null when any argument is statically non-null. Narrow a direct field in the same
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
can become `Decimal(12, 2)` when its other argument and output target establish that context. Use `to_decimal`,
`to_date`, and `to_timestamp` for semantic parsing conversions.

## Runtime Shape And Validation

Generated `*_SCHEMA` constants and online result schemas are equivalent shape-only `StructType` artifacts. They include
effective Spark names, order, data types, nullability, and nested shape. Callers may use them with `spark.read.schema`,
their own validation, and pre-write projection. They do not execute value-level constraints.

Runtime validation has input, intermediate, and output phases. Its modes are `off`, `schema_only`, and
`schema_and_constraints`. The default is schema-only for every phase, with intermediate validation enabled. Resolution
proceeds from defaults through project files and CLI flags, then transform and method overrides, then hook-local schema
policy.

Schema-only validation checks required and unexpected columns, strict order where needed, Spark types, nullability where
Spark reports it reliably, nested structs, array elements, and map keys/values. It does not call `count`, `collect`,
`toPandas`, sample rows, or add actions. Strict mode rejects extra columns. A hook may opt into
`SchemaMode.ALLOW_EXTRA_COLUMNS`; `project_output=True` then restores target columns and order. Online and generated
execution validate at identical boundaries.

### Phases, Defaults, And Placement

Input validation checks supplied DataFrames. Intermediate validation checks every compiled step and its attached hooks.
Output validation checks final returned frames. The default policy is schema-only at all phases:

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

Schema-only validation checks required columns, extras in strict mode, target order where required, Spark types,
reliable nullability metadata, nested structs, arrays, and maps. It never calls `count`, `collect`, `toPandas`, samples
rows, or performs a data aggregation. Input validation precedes the first step; step and hook validation occurs at its
declared boundary; final validation precedes the result. Streaming DataFrames can use schema-only validation because it
inspects metadata, but a data-quality constraint needs its own streaming admission rule.

## Data Quality Constraints

Schema shape and data quality are distinct. Accepted values, ranges, patterns, decimal domains, uniqueness, referential
checks, freshness, and row-count rules are value or dataset facts that can require filters, aggregation, joins, or
actions. They are explicit, phase-bound, and cost-classified rather than silently enabled by a schema declaration.

`schema_and_constraints` reserves opt-in validation for such checks. Until concrete families are implemented, it may
report that only schema checks are available. Generated schemas remain shape-only; future constraint metadata must use
separate artifacts unless a later design explicitly changes this contract. Schema-only validation can be streaming-safe;
each data-quality constraint needs its own streaming admission rule.

### Constraint Families And Cost

The planned field-local families are accepted values, numeric and temporal ranges, patterns, length limits, and decimal
domains. Schema-level families are unique keys, cross-field conditions, row-count bounds, and freshness. Cross-dataset
families are referential and anti-existence checks.

Each constraint must declare its target, phases, kind, severity, source location, and cost: compile-time only,
schema-only, row-local, aggregation-based, or join-based. Cost determines default eligibility and streaming support.
Storage orchestration remains caller-owned even where Structure supplies generated schemas for caller-owned reads and
writes.

## Diagnostics

Schema diagnostics name the schema, field, phase, source expression or DataFrame column, applicable target policy, the
problem, and the shortest correction. They cover declaration syntax, invalid types, duplicate effective names,
inheritance ambiguity, missing constructor fields, nullable assignments, explicit parsing, incompatible types, and
runtime shape mismatch.

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

## More Details

- [Schema declaration syntax](../background/SchemaDeclarationSyntax.back.md) contains declaration and migration detail.
- [Schema model](../background/SchemaModel.back.md) contains compiler-model and Spark-mapping detail.
- [Schema inheritance](../background/SchemaInheritance.back.md) contains worked composition examples.
- [Schema semantics](../background/SchemaSemantics.back.md) records the original consolidated semantic contract.
- [Nullability and type coercion](../background/NullabilityAndTypeCoercion.back.md) contains expression typing detail.
- [Validation semantics](../background/ValidationSemantics.back.md) contains phase and hook validation detail.
- [Data quality constraints](../background/DataQualityConstraints.back.md) defines the deferred value-quality boundary.
