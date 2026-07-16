# Schema Model

The schema model is the Spark-free representation of a discovered `Schema` class. It is the shared source for compiler
checks, online execution, generated Spark schemas, validation, diagnostics, traceability, and generated docs.

See [Schema Declaration Syntax](SchemaDeclarationSyntax.back.md) for authoring syntax and the
[Schema reference](../reference/Schema.ref.md) for supported behavior.

## Core Shape

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
```

`fields` is the effective ordered list after inheritance. `local_fields` contains declarations written directly on the
class. Field order determines generated schema, projection, validation, and documentation order.

## Extraction

```text
Schema class
  -> field-factory capture
  -> inheritance resolution
  -> type validation
  -> SchemaDef
  -> compiler checks
  -> generated Spark schema / runtime validation
```

Extraction rejects invalid field-factory options, invalid Decimal precision or scale, invalid nested types, recursive
struct cycles, ambiguous inherited fields, non-schema bases, and duplicate effective Python or Spark column names.

Aliases select the physical Spark column name; otherwise it is the Python field name. Metadata and descriptions are
compiler metadata and do not alter Spark shape. Schema declarations do not record primary keys or uniqueness proofs.

## Type Model

Field factories create immutable Structure types for strings, numeric values, booleans, dates, timestamps, Decimals,
arrays, maps, and nested structs. Type equality is structural, while schema identity remains nominal: two schema classes
with the same fields are still distinct contracts unless a specific operation defines compatibility.

The compiler validates the model without importing PySpark, starting Java, creating a `SparkSession`, or inspecting
live data. Generated and online paths consume this same model to preserve schema parity.
