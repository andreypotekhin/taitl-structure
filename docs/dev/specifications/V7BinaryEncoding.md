# V7 Binary Values and Encoding

## Purpose

This specification adds a first-class Binary field type so compiler-visible PySpark encoding operations can preserve
their input/output types instead of requiring `@raw` hooks.

## Public API

`binary(nullable=...)` declares Binary fields. The scalar helpers are:

    base64(value)
    unbase64(value)
    encode(value, charset="UTF-8")
    decode(value, charset="UTF-8")

`base64` accepts Binary and returns nullable String. `unbase64` accepts String and returns nullable Binary.
`encode` accepts String and returns nullable Binary. `decode` accepts Binary and returns nullable String. `charset` is a
non-empty literal canonical charset name; it is neither a Column expression nor an arbitrary callable.

## Semantics

Input null propagates. Invalid base64 and malformed byte decoding must have one verified cross-target outcome before
support is claimed; if PySpark 3.5 and 4.0 disagree, the helper stays capability-gated rather than hiding the difference.
Binary values support schema validation, nested struct/array/map declarations, generated schemas, and normal projection.
They do not introduce file I/O, arbitrary codecs, encryption, compression, or driver-side byte handling.

## Compiler Contract

The type mapper, expression type checks, capability model, recipe, online evaluator, and renderer all carry Binary
explicitly. Rendered code uses only `pyspark.sql.functions` calls and literal charset arguments. Explain and traceability
record the operation family but do not log data values or decoded content.

## Evidence

Tests cover type rejection, literal charset validation, nullable propagation, nested Binary schemas, generated source,
online/generated parity, and live PySpark 3.5/4.0 behavior for valid and invalid values. The catalog changes to supported
only after those results are recorded.
