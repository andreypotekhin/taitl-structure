# Schema Semantics

## Purpose

Structure schemas define row shape and type meaning for compiler checks, generated Spark schemas, runtime validation,
online execution, generated code, diagnostics, and lineage. This specification ties together schema declaration syntax,
schema model extraction, inheritance, output construction, nullability, and assignment compatibility into one
implementation-ready semantic contract.

Detailed syntax remains owned by:

- `docs/specifications/SchemaDeclarationSyntax.md`;
- `docs/specifications/SchemaModel.md`;
- `docs/specifications/SchemaInheritance.md`;
- `docs/specifications/NullabilityAndTypeCoercion.md`;
- `docs/specifications/DataQualityConstraints.md`.

## Semantic Layers

Structure schema behavior has four layers:

1. Source declarations: Python classes that inherit `Structure` and declare fields with `field(...)`.
2. Compiler model: backend-neutral `SchemaDef`, `FieldDef`, and type values.
3. Runtime shape: generated or materialized Spark `StructType` values.
4. Value constraints: future explicit data-quality checks outside the base shape model.

The schema model is the source of truth. Generated PySpark schemas and online materialized schemas are derived
artifacts.

## Canonical Declaration

The canonical v1 declaration form is:

```python
class OrderRaw(Structure):
    id = field(String(), nullable=False, primary_key=True)
    customer_id = field(String(), nullable=False)
    total = field(String(), nullable=True)
```

Rules:

- Every field uses `field(type_, ...)`.
- Every type is an explicit immutable Structure type object.
- Field order is class-body order after inheritance is resolved.
- Field names are Python attribute names.
- `primary_key=True` implies `nullable=False`.
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
primary_key
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
String()
Integer()
Long()
Float()
Double()
Decimal(precision, scale)
Boolean()
Date()
Timestamp()
Array(type_, contains_null=True)
Struct(SchemaClass)
Map(key_type, value_type, value_contains_null=True)
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
- Effective inherited fields precede local fields unless overridden according to `SchemaInheritance.md`.
- Ambiguous inherited fields are rejected.
- Non-schema bases are rejected unless a later spec introduces allowed mixins.
- Field origin metadata is retained for diagnostics and generated documentation.

## Output Construction

Inside a compiled subtransform, calling a schema class creates a symbolic projection into that schema:

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
- Assignment type and nullability are checked before generated or online runtime execution.

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
- `where(expr.is_not_null())` narrows simple field references after the filter in the same subtransform.
- `Join.LEFT` makes joined right-side fields nullable after the join.
- `Join.INNER` preserves right-side declared nullability unless later operations narrow it.
- Hooks do not provide compile-time nullability facts unless a later hook postcondition contract exists.

## Runtime Shape

Generated schema constants and online materialized schemas are shape-only Spark `StructType` artifacts.

Rules:

- They include field names, field order, Spark data types, nullability, and nested shape.
- They do not include future value-level constraints as executable behavior.
- They may be used by caller code for `spark.read.schema(...)`, validation, and pre-write projection.
- Online execution exposes equivalent schemas after `.run(session)` without requiring generated files.

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
  The output constructor does not provide a value for the target field.

Use:
  Add total=... to the constructor or copy it through OrderNormalized.base(source) when compatible.

See docs/specifications/SchemaSemantics.md
```

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
10. Materialize equivalent Spark schemas during online execution.
11. Keep generated schema constants shape-only.
12. Add schema diagnostics with documentation links.

## Acceptance Criteria

- A valid schema declaration emits a deterministic `SchemaDef`.
- Primitive, decimal, array, map, and nested struct fields are represented without PySpark imports.
- Inherited schemas preserve effective field order and origin metadata.
- Invalid type declarations fail during compiler commands.
- Output constructors reject missing, unknown, incompatible, or nullable-to-non-nullable fields.
- Base overlays copy only compatible fields and preserve explicit override semantics.
- Generated Spark schemas and online materialized schemas are equivalent.
- Schema-only validation uses schema shape and does not scan rows.
- Diagnostics link to this document or a narrower schema specification.
