# V7 Schema-Carrying JSON and CSV Conversion

## Purpose

This specification admits safe PySpark JSON and CSV column conversion by requiring a declared Structure Schema and a
small normalized option record. It does not admit schema inference or arbitrary parser configuration.

## Public API

    from_json(value, as_=Record, options=JsonOptions())
    to_json(value, options=JsonOptions())
    from_csv(value, as_=Record, options=CsvOptions())
    to_csv(value, options=CsvOptions())

`from_json` and `from_csv` accept String and return the exact `Struct[Record]` type. Because Spark 3.5 and 4.0
materialize permissively parsed struct fields as nullable, the declared parser Schema must mark every parsed field
nullable. `to_json` and `to_csv` accept a typed Struct and return nullable String. `JsonOptions` and `CsvOptions` are
immutable literal-only records. Their first release supports delimiter, quote, escape, null value, date format,
timestamp format, and permissive parsing mode.

## Semantics

No helper infers a schema. No options argument accepts a dictionary, Column, callback, or unknown key. The parser result
and nested field nullability follow the declared Schema plus the documented behavior of the selected permissive mode.
Malformed input must be represented by the exact verified nullable result/field behavior; a profile disagreement is a
capability diagnostic, not an unannounced generated-code difference.

JSON map/array/variant result shapes, CSV schema inference, file reading, multiline/source options, and streaming claims
remain outside this release.

## Compiler Contract and Evidence

The compiler maps a Structure Schema into the target schema argument and serializes normalized options into immutable
recipes. JSON parsing uses a `StructType` schema argument. CSV parsing uses a DDL schema string because PySpark 3.5/4.0
`from_csv` accepts schema strings or schema columns, not Python `StructType` values. Online and generated rendering use
the same options and public PySpark functions. Tests cover option rejection, result shape, nullability, malformed input,
nested records, generated source, parity, diagnostics, and live PySpark 3.5/4.0 evidence.
