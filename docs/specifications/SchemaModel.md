# Schema Model

This specification replaces `docs/dev/design/SchemaModel.md` as the implementation-level schema model reference.

## Purpose

The schema model represents user-declared data structures independently from PySpark. It is the source of truth for
compiler checks, generated Spark `StructType` code, runtime validation, lineage, documentation, and IDE-oriented
diagnostics.

Schema declarations are authored with the syntax specified in
`docs/specifications/SchemaDeclarationSyntax.md`. Inheritance behavior is specified in
`docs/specifications/SchemaInheritance.md`.

## Core Model

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

## SchemaDef Rules

`SchemaDef` represents one discovered `Schema` subclass.

Rules:

- `name` is the class name.
- `qualified_name` is the importable module-qualified class name.
- `module` is the source module name.
- `source_path` and `source_line` are included when available.
- `bases` lists direct schema bases in class definition order.
- `fields` contains effective fields in generated output order.
- `local_fields` contains fields declared directly on the class.
- `constraints` contains schema-level constraints when introduced by later specs.
- `metadata` is immutable.

Each schema class has a distinct schema identity. Two schemas with identical fields are structurally compatible, but not
the same schema.

## FieldDef Rules

`FieldDef` represents one effective schema field.

Rules:

- `name` is the Python class attribute name.
- `type` is a Structure `TypeDef`.
- `nullable` defaults to `True`.
- `primary_key` defaults to `False`.
- `primary_key=True` implies `nullable=False`.
- `metadata` is immutable and defaults to empty.
- `description` is optional.
- `declaring_schema` is the schema class that declared the effective field.
- `owning_schema` is the schema whose effective field list contains this field.
- `inherited` is true when `declaring_schema != owning_schema`.
- `overrides` points to the overridden field origin when the field replaces an inherited field.

Field order is part of the schema contract. Generated Spark schemas and projections must use `SchemaDef.fields` order.

## Type Model

All type objects are immutable structural values.

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

`MapType` is included in v1 for schema declaration, Spark schema generation, and runtime validation. Higher-order map
transformations remain a v2 expression feature.

Type equality is structural:

```text
String() == String()
Decimal(12, 2) == Decimal(12, 2)
Array(String()) == Array(String())
Struct(Address) == Struct(Address)
```

## Supported Types v1

- `String()`
- `Integer()`
- `Long()`
- `Float()`
- `Double()`
- `Decimal(precision, scale)`
- `Boolean()`
- `Date()`
- `Timestamp()`
- `Struct(SchemaClass)`
- `Array(type_)`
- `Map(key_type, value_type)`

## Extraction Flow

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

Extraction must not import PySpark or create a Spark session.

## Inheritance Integration

Schema inheritance is resolved before checks or generation.

Rules:

- Use `SchemaInheritance.spec.md` to build `SchemaDef.fields`.
- Use `SchemaDef.local_fields` for source documentation and override diagnostics.
- Use `SchemaDef.fields` for generated schemas, runtime validation, and output projection order.
- Retain field origin information for diagnostics and lineage.

Example:

```python
class EntityKeys(Schema):
    id = field(String(), nullable=False, primary_key=True)


class Order(EntityKeys):
    total = field(Decimal(12, 2), nullable=True)
```

Effective `Order` fields:

```text
id     declaring_schema=EntityKeys  inherited=True
total  declaring_schema=Order       inherited=False
```

## Schema Declaration Example

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

## Spark Type Mapping

```text
String()             -> T.StringType()
Integer()            -> T.IntegerType()
Long()               -> T.LongType()
Float()              -> T.FloatType()
Double()             -> T.DoubleType()
Decimal(12, 2)       -> T.DecimalType(12, 2)
Boolean()            -> T.BooleanType()
Date()               -> T.DateType()
Timestamp()          -> T.TimestampType()
Array(String())      -> T.ArrayType(T.StringType(), containsNull=True)
Struct(Address)      -> T.StructType([...])
Map(String(), Long()) -> T.MapType(T.StringType(), T.LongType(), valueContainsNull=True)
```

Spark schema generation must be deterministic and formatted consistently.

## Validation Rules

Schema extraction must reject:

- non-Structure type values in `field(...)`;
- invalid decimal precision or scale;
- invalid nested type expressions;
- recursive struct cycles;
- ambiguous inherited fields;
- non-schema bases;
- duplicate effective field names after inheritance resolution;
- unsupported field declaration shapes.

Errors should link to the most specific relevant spec.

## Runtime Model

Generated runtime validation uses `SchemaDef` through generated Spark schemas.

Runtime validation checks:

- required columns;
- unexpected columns when validation mode is strict;
- Spark data types;
- nullability when Spark metadata is reliable;
- nested struct shape;
- array element type where available.
- map key and value types where available.

Row-level constraint validation is outside the base schema model and belongs to validation semantics.

## Compile-Time Performance

Schema extraction should be cacheable by source fingerprint.

Targets:

- extraction should not import PySpark, start Java, create a SparkSession, or contact a Spark cluster;
- type objects should be lightweight immutable values;
- inheritance resolution should be linear in the number of schema classes plus field declarations;
- generated Spark `StructType` text should be deterministic and cheap to emit.

## Implementation Checklist

1. Implement immutable type model values.
2. Implement `SchemaDef` and `FieldDef`.
3. Capture local field order during schema class creation.
4. Resolve inheritance into effective field order.
5. Validate field types and nested type expressions.
6. Retain source location and origin metadata where available.
7. Generate Spark `StructType` definitions from `SchemaDef.fields`.
8. Support runtime validation using generated Spark schemas.
9. Add diagnostics with links to relevant specifications.
10. Add tests for primitive, decimal, array, map, struct, and inherited schemas.

## Acceptance Criteria

- A simple schema produces a `SchemaDef` with ordered fields.
- An inherited schema produces a `SchemaDef` with effective inherited fields.
- Field origin metadata is retained for inherited and overridden fields.
- Generated Spark `StructType` field order matches `SchemaDef.fields`.
- Schema extraction works without PySpark, Java, a SparkSession, or Spark startup.
- Invalid field declarations fail during `structure check` with actionable diagnostics.
