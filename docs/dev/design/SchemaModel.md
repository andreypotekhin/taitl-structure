# Design: Schema Model

## Purpose

The schema model represents user-declared data structures independently from PySpark. It is the source of truth for compiler checks, generated Spark `StructType`, runtime validation, and lineage.

## Core Types

```text
SchemaDef
  name
  qualified_name
  fields
  constraints
  metadata

FieldDef
  name
  type
  nullable
  primary_key
  metadata
```

## Supported Types v1

- string
- integer
- long
- decimal
- boolean
- date
- timestamp
- nested struct
- array
- map, if needed for v1; otherwise v2 with HOFs

## Schema Declaration Example

```python
class Customer(Schema):
    id = field(string, nullable=False, primary_key=True)
    name = field(string, nullable=True)
    tier = field(string, nullable=True)
```

## Generated PySpark Schema

```python
CUSTOMER_SCHEMA = T.StructType([
    T.StructField("id", T.StringType(), nullable=False),
    T.StructField("name", T.StringType(), nullable=True),
    T.StructField("tier", T.StringType(), nullable=True),
])
```

## Data Flow

```text
Schema class
  ↓ discovery
SchemaDef
  ↓ checks
schema constraints and field validation
  ↓ generator
Spark StructType module
  ↓ runtime
assert_schema(df, schema)
```

## Compile-Time Performance

Schema extraction should be cacheable by source fingerprint. Generating Spark `StructType` text should be deterministic and cheap. Schema objects should not import PySpark during compiler phases unless emitting code.
