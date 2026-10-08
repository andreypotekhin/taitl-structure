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

## Admitted scalar contract

The first scalar slice uses existing `call` expression nodes and scalar types:

| Function | Typed operands | Result | Nullability | Determinism / streaming |
| --- | --- | --- | --- | --- |
| `chr` | Integer or Long, including integer literals | String | Input nullability | Deterministic; stateless, compatible by design |
| `quote` | String, including string literals | String | Always nullable, matching native Spark | Deterministic; stateless, compatible by design |
| `try_to_date` | String, Date, or LTZ Timestamp; optional non-empty pattern literal | Date | Always conservatively nullable | Deterministic for fixed session configuration; stateless, compatible by design |
| `random` | Literal integer seed or `reproducible=False` | Double | Non-null | Nondeterministic; batch-only |
| `uuid` | Literal integer seed or `reproducible=False` | String | Non-null | Nondeterministic; batch-only |

All five require exact `>=4.1,<4.2` ordinary or Connect admission. Names use native generated spelling and individual
`expression.<name>` capability requirements. `chr` shares `char`'s typed constructor; `random` and `uuid` share the
existing random seed policy. Seeded results have parity for the same input partitioning; no cross-partition or
cross-version stability promise is made. Random recipes carry the nondeterminism marker so projection/union
optimization cannot certify them as deterministic. Streaming classification checks nested recipes, projections,
filters, and special expression bodies for the batch-only helpers.

`try_to_date` keeps parse-failure nullability even after a non-null source filter. A literal pattern is rendered as
a Python string, matching the native API. A pattern on Date/LTZ Timestamp input emits `PYSPARK-W2705` because Spark
ignores it. Invalid pattern definitions remain Spark errors. LTZ conversion follows the session time zone; NTZ inputs
remain outside this helper's admitted operand set. Ordinary and Connect batch evidence is required before promotion;
the deterministic helpers' streaming classification is compatible by design, with no live streaming claim.

## TIME contract

The public TIME contract is approved for V11. `time(precision=6, ...)` declares Spark TIME with precision 0 through 6;
precision is part of type/schema identity and survives nested schema rendering, materialization, and schema reads.
Python `datetime.time` literals preserve their clock fields, including for timezone-aware values (Spark ignores
`tzinfo`).

The admitted 4.1 helpers are `current_time(precision=6)`, `make_time(hour, minute, second)`,
`to_time(value, format=None)`, `try_to_time(value, format=None)`, `time_diff(unit, start, end)`, and
`time_trunc(unit, value)`. `current_time` is non-null and query-stable; `make_time`, `to_time`, and `try_to_time` return
nullable TIME(6); `time_diff` returns nullable Long; `time_trunc` returns nullable TIME at the input precision. Formats
and units accept literal or typed String inputs; `time_diff` units are HOUR, MINUTE, SECOND, MILLISECOND, and
MICROSECOND, case-insensitive. Spark owns input validation and parse errors, including strict `to_time` failures in
both ANSI modes.

TIME comparisons require matching precisions, while ordering accepts TIME values at any precision. TIME-to-TIME
precision casts and TIME/String casts are supported. Date and
timestamp casts and TIME arithmetic remain outside scope. Spark's `spark.sql.timeType.enabled` flag is caller-owned;
Structure neither enables nor checks it and exposes Spark's native disabled-type failure. Each helper and the schema
type require exact `>=4.1,<4.2` ordinary or Connect capability. Batch live evidence is required for both variants;
this contract makes no streaming claim.

## Evidence

For every supported function group, test null input, boundary values, nested arrays/maps where applicable, malformed
input, output schema, online/generated equality, generated code spelling, and capability rejection on 3.5/4.0. Run the
positive cases on ordinary 4.1 and on Connect 4.1 only when the API is documented and proven there.
