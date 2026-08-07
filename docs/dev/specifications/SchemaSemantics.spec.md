# Schema Semantics

## Purpose

Structure schemas define row shape and type meaning for compiler checks, generated Spark schemas, runtime validation,
execution, generated code, diagnostics, and traceability. This specification ties together schema declaration
syntax, schema model extraction, inheritance, output construction, nullability, and assignment compatibility into one
schema semantics reference.

Detailed syntax remains owned by:

- [SchemaDeclarationSyntax.spec.md](SchemaDeclarationSyntax.spec.md);
- [SchemaModel.spec.md](SchemaModel.spec.md);
- [SchemaInheritance.spec.md](SchemaInheritance.spec.md);
- [NullabilityAndTypeCoercion.spec.md](NullabilityAndTypeCoercion.spec.md);
- [DataQualityConstraints.spec.md](DataQualityConstraints.spec.md).

## Semantic Layers

Structure schema behavior has four layers:

1. Source declarations: Python classes that inherit `Schema` and declare PySpark fields with Python hints,
   `structure.plugin.pyspark` factories, or both.
2. Compiler model: backend-neutral `SchemaDef`, `FieldDef`, and type values.
3. Runtime shape: generated or materialized Spark `StructType` values.
4. Value constraints: future explicit data-quality checks outside the base shape model.

The schema model is the source of truth. Generated PySpark schemas and execution-materialized schemas are derived
artifacts.

## Canonical Declaration

The canonical v1 declaration form is:

```python
from structure import Schema
from structure.plugin.pyspark import *


class OrderRaw(Schema):
    id: str = string(nullable=False)
    customer_id: str = string(nullable=False)
    total: str
```

Rules:

- A supported bare Python hint infers the matching default PySpark factory; an explicit factory may add Spark detail.
- Every factory produces an immutable Structure type value and its field declaration, and must be compatible with a
  field hint when one is supplied.
- Field order is class-body order after inheritance is resolved.
- Field names are Python attribute names.
- Public examples must use this form.

## Schema Identity

A schema class defines a named row contract. Two schemas with identical fields may be structurally compatible, but they
are not the same schema identity.

Rules:

- `SchemaDef.qualified_name` is the stable compiler identity for a schema class.
- Source path and line number are diagnostic metadata, not semantic identity.
- Renaming a schema class or moving it to another module changes identity.
- Generated schema constant names are derived deterministically from schema identity and local naming rules.

## Field Semantics

Each field has:

```text
name
type
nullable
metadata
description
declaring_schema
owning_schema
source location
```

Rules:

- Effective field order is the output projection order.
- Missing fields are validation failures.
- Extra DataFrame columns are failures in strict validation mode.
- Unknown field constructor keywords are declaration errors.
- Field metadata and descriptions do not change Spark shape semantics unless a narrower spec says so.
- Future aliases must not be added without a migration specification because generated code and diagnostics rely on
  field names.

## Type Semantics

v1 schema types:

```text
string()
integer()
long()
float()
double()
decimal(precision, scale)
boolean()
date()
timestamp()
array(field_factory, contains_null=True)
struct(SchemaClass)
map(field_factory, field_factory, value_contains_null=True)
```

Rules:

- Type objects are immutable and structurally comparable.
- Decimal precision and scale must be valid before a `SchemaDef` is emitted.
- Nested struct cycles are rejected.
- Map keys are never nullable because Spark map keys cannot be null.
- Higher-order array and map transformations are not implied by declaring array or map fields.

## Inheritance

Schema inheritance is schema-to-schema reuse, not arbitrary Python mixin behavior.

Rules:

- Direct schema bases are processed in class declaration order.
- Effective inherited fields precede local fields unless overridden according to `SchemaInheritance.spec.md`.
- Ambiguous inherited fields are rejected.
- Non-schema bases are rejected unless a later spec introduces allowed mixins.
- Field origin metadata is retained for diagnostics and generated documentation.

## Output Construction

Inside a compiled step method, calling a schema class creates a symbolic projection into that schema:

```python
return OrderNormalized(
    id=order.id,
    customer_id=lower(trim(order.customer_id)),
    total=to_decimal(order.total, precision=12, scale=2),
)
```

Rules:

- Positional arguments are rejected.
- Unknown keyword fields are rejected.
- All target fields must be supplied or copied through a specified base overlay.
- Projection order follows the target schema, not source keyword order.
- Assignment type and nullability are checked before generated or direct runtime execution.

Base overlay syntax copies compatible inherited fields:

```python
return OrderWithCustomer.base(order)(
    customer_name=customer.name,
    customer_tier=customer.tier,
)
```

Rules:

- Explicit overrides win.
- Extra source fields are ignored.
- Missing target fields are errors.
- Copied fields must be type- and nullability-compatible.

## Nullability

Nullability is part of every field and every expression.

Rules:

- A nullable expression cannot feed a non-nullable target unless narrowed or repaired.
- `where(expr.is_not_null())` narrows simple field references after the filter in the same step method.
- `"left"` makes joined right-side fields nullable after the join.
- `"inner"` preserves right-side declared nullability unless later operations narrow it.
- Hooks do not provide compile-time nullability facts unless a later hook postcondition contract exists.

## Runtime Shape

Generated schema constants and execution-materialized schemas are shape-only Spark `StructType` artifacts.

Rules:

- They include field names, field order, Spark data types, nullability, and nested shape.
- They do not include future value-level constraints as executable behavior.
- They may be used by caller code for `spark.read.schema(...)`, validation, and pre-write projection.
- Execution exposes equivalent schemas after `.run(session)` without requiring generated files.

## Diagnostics

Schema diagnostics must include:

- schema class;
- field name when relevant;
- expected type and nullability;
- actual declaration or expression metadata;
- source location when available;
- problem;
- suggested fix;
- link to the most specific schema specification.

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

See docs/dev/specifications/SchemaSemantics.spec.md
```

## Binary Fields

`binary(nullable=...)` declares a first-class Binary field. Binary values participate in nested Struct, Array, and Map
declarations, generated schemas, validation, and ordinary projections. They are data values, not file or driver-side
byte-processing abstractions.

The typed scalar helpers are `base64(value)`, `unbase64(value)`, `encode(value, charset="UTF-8")`, and
`decode(value, charset="UTF-8")`. Base64 accepts Binary and returns nullable String; unbase64 accepts String and
returns nullable Binary; encode accepts String and returns nullable Binary; decode accepts Binary and returns nullable
String. Charset names are non-empty literal canonical names. Invalid base64 and malformed decoding remain capability-
gated until PySpark target behavior agrees.

The type mapper, expression checks, capability model, recipes, online evaluator, renderer, explain output, and
traceability must carry Binary explicitly. Generated code uses public PySpark functions and never logs binary contents.

## Schema-Carrying JSON and CSV Conversion

`from_json(value, as_=Schema, options=...)` and `from_csv(value, as_=Schema, options=...)` return the exact declared
Struct shape. `to_json(value, options=...)` and `to_csv(value, options=...)` accept typed Struct values and return
nullable String. Parser schemas are never inferred. Because permissive parsing materializes parsed fields as nullable
on the supported PySpark profiles, parser Schemas must declare every parsed field nullable.

`JsonOptions` and `CsvOptions` are immutable literal-only records. The initial option set is delimiter, quote, escape,
null value, date format, timestamp format, and permissive parse mode. Dictionaries, Columns, callbacks, unknown keys,
dynamic option values, map/variant results, schema inference, file options, and streaming claims are outside this
contract. Malformed input follows the exact verified nullable behavior of the selected target profile; disagreement is
a capability diagnostic rather than an unannounced execution difference.

JSON and CSV conversion must lower through the shared recipes. JSON uses a generated `StructType` schema argument; CSV
uses the target-compatible DDL schema form. Tests cover options, nested records, nullability, malformed values,
generated source, online/generated parity, diagnostics, and live PySpark 3.5/4.0 behavior.

## Implementation Checklist

1. Implement immutable type objects.
2. Capture field declarations and class-body order.
3. Resolve inheritance into effective fields.
4. Build `SchemaDef` and `FieldDef` values.
5. Validate type expressions, duplicate fields, decimal bounds, and nested cycles.
6. Implement symbolic schema construction and base overlays.
7. Attach type and nullability to expressions.
8. Check output assignment compatibility.
9. Generate Spark `StructType` constants from `SchemaDef`.
10. Materialize equivalent Spark schemas during execution.
11. Keep generated schema constants shape-only.
12. Add schema diagnostics with documentation links.

## Acceptance Criteria

- A valid schema declaration emits a deterministic `SchemaDef`.
- Primitive, decimal, array, map, and nested struct fields are represented without PySpark imports.
- Inherited schemas preserve effective field order and origin metadata.
- Invalid type declarations fail during compiler commands.
- Output constructors reject missing, unknown, incompatible, or nullable-to-non-nullable fields.
- Base overlays copy only compatible fields and preserve explicit override semantics.
- Generated Spark schemas and execution-materialized schemas are equivalent.
- Schema-only validation uses schema shape and does not scan rows.
- Diagnostics link to this document or a narrower schema specification.
