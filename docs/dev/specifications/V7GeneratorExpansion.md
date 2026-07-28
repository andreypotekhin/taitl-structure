# V7 Typed Generator Expansion

## Purpose

This specification broadens Structure's row-expansion support while retaining explicit output Schemas and cardinality.
Every generator consumes the current relation, produces a new declared relation, and remains visible to compilation,
execution, generated rendering, explain, and traceability.

## Common Rules

- Every generator accepts a typed `array<struct>` expression with `contains_null=False`.
- `as_` is a declared Structure Schema for the generated scope; runtime field discovery is forbidden.
- `scope` is a non-empty, unique symbolic relation name. Generated output field names come from `as_`, not data.
- Inner variants emit zero rows for null or empty arrays. Outer variants emit one row with null generated fields.
- Generator operations multiply or preserve rows as stated below and invalidate a preceding relation-order claim.
- All forms are batch-only. A streaming transform receives the existing batch-only generator diagnostic.
- Scalar-array and map generators remain deferred because their public output naming contracts differ.

## Public Helpers

`posexplode_struct(...)` remains compatible. V7 adds:

    explode_struct(value, as_=Generated, scope="item")
    explode_outer_struct(value, as_=Generated, scope="item")
    posexplode_outer_struct(value, as_=Generated, ordinal="ordinal", scope="item")
    inline_struct(value, as_=Generated, scope="item")
    inline_outer_struct(value, as_=Generated, scope="item")

`explode_struct` and `explode_outer_struct` require `as_` to contain exactly the element struct fields. The outer form
requires those fields to be nullable in the generated Schema. `posexplode_outer_struct` additionally requires a nullable
Long ordinal field. `inline_struct` and `inline_outer_struct` use the element struct fields as declared sibling output
fields; their scope still preserves provenance and field lookup rules.

## Compiler Contract

Each operation records its kind, expression, generated Schema, optional ordinal, scope, cardinality, and outer flag in
an immutable operation plan and PySpark recipe. Capability, explain, and traceability records identify the precise kind.
Generated and online paths use public PySpark generator functions, never raw SQL, Python UDFs, actions, or a driver loop.

## Diagnostics

Compilation rejects a scalar or map source, nullable element values, an incompatible generated Schema, a missing/wrong
ordinal, a scope collision, and use inside a scalar lambda, aggregate assignment, window expression, or streaming step.

## Evidence

For every helper, tests cover null, empty, one-element, multi-element, nested field, schema mismatch, generated source,
online/generated parity, explain, traceability, and classic PySpark 3.5/4.0 execution. The original
`posexplode_struct(...)` tests are the characterization baseline for delegate extraction.
