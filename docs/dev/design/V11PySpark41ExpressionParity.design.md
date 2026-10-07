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

The pinned Python function-index delta has 43 new names: two Arrow callback decorators, two string functions, seven
temporal functions, two random functions, five native geospatial functions, and 25 KLL/Theta sketch functions. The
sketch and geospatial owners have their own design contracts. `random` and `try_to_date` first appear in the 4.1 index
although their individual 4.1 pages annotate earlier introduction versions; `uniform` and `randstr` are already in the
4.0 index. Existing Structure helpers are reused
when their semantics already match; new names do not create duplicate quasi-equivalent nodes.

## Decisions to make in implementation

`Column.transform` is admitted as a whole-expression transformation for PySpark 4.1 ordinary and Connect variants. Its callback receives one
typed expression, executes during symbolic authoring, and returns the typed result expression; the result may have a
different type and nullability. Generated and online execution lower the callback to PySpark's `Column.transform`.
Both 4.1 variants have live online/generated evidence, including nullable input and changed result type. Random helpers require a seed or an explicit
nondeterminism marker and are batch-only until a streaming policy exists. Sketch functions are owned by the
observations-and-sketches design. Any function whose result depends on session configuration, collation, locale, or
opaque SQL text needs a separate type and configuration contract.

## Evidence

For every supported function group, test null input, boundary values, nested arrays/maps where applicable, malformed
input, output schema, online/generated equality, generated code spelling, and capability rejection on 3.5/4.0. Run the
positive cases on ordinary 4.1 and on Connect 4.1 only when the API is documented and proven there.
