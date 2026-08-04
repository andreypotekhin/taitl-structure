# V11 PySpark 4.1 Expression Parity Design

## Purpose

Define the typed contract for PySpark 4.1 row-preserving functions and `Column.transform`. This document is deliberately
narrow: a function is supportable only when its result type, nullability, determinism, generated spelling, and streaming
behavior are known.

## Contract

The expression IR records the operation name, typed operands, result type, nullability rule, target profile, target
variant, and determinism. The online evaluator and generated renderer consume the same operation record. Higher-order
callbacks are symbolic expressions over a declared element type; Python code is never executed once per row during
compilation or runtime.

The inventory classifies the 4.1 built-in additions into deterministic scalar/collection functions, seeded random
functions, aggregate/sketch functions, and APIs outside the expression boundary. Existing Structure helpers are reused
when their semantics already match; new names do not create duplicate quasi-equivalent nodes.

## Decisions to make in implementation

`Column.transform` may be admitted for arrays only, with a one-element-in/one-element-out contract and unchanged row
cardinality. Random helpers require a seed or an explicit nondeterminism marker and are batch-only until a streaming
policy exists. Sketch functions are owned by the observations-and-sketches design. Any function whose result depends on
session configuration, collation, locale, or opaque SQL text needs a separate type and configuration contract.

## Evidence

For every supported function group, test null input, boundary values, nested arrays/maps where applicable, malformed
input, output schema, online/generated equality, generated code spelling, and capability rejection on 3.5/4.0. Run the
positive cases on ordinary 4.1 and on Connect 4.1 only when the API is documented and proven there.
