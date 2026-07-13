# Schema Semantics

Structure schemas define row shape and type meaning for compiler checks, generated Spark schemas, runtime validation,
online execution, generated code, diagnostics, and traceability. This reference ties together schema declaration
syntax, schema model extraction, inheritance, output construction, nullability, and assignment compatibility into one
schema semantics reference.

Detailed syntax remains owned by:

- [SchemaDeclarationSyntax.md](SchemaDeclarationSyntax.md);
- [SchemaModel.md](SchemaModel.md);
- [SchemaInheritance.md](SchemaInheritance.md);
- [NullabilityAndTypeCoercion.md](NullabilityAndTypeCoercion.md);
- [DataQualityConstraints.md](DataQualityConstraints.md).

See the exhaustive [schemas API table](../api/Schemas.api.md) for supported declaration names and examples.

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
class OrderRaw(Schema):
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
- Future aliases must not be added without a migration reference because generated code and diagnostics rely on
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
- `Struct(Address)` and `Struct(BillingAddress)` are compatible only when they reference the same schema class.
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
- Assignment type and nullability are checked before generated or online runtime execution.

Nested `Struct(...)` fields may be assigned by copying a whole compatible struct expression or by constructing the
nested schema explicitly:

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
- Partial nested updates such as replacing only `shipping.city` are deferred planned work; construct or copy the whole
  nested value in this slice.

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
- `where(parent_struct.is_not_null())` narrows nested reads through that parent according to each nested field's own
  declared nullability.
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

See docs/reference/SchemaSemantics.md
```
