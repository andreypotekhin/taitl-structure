# V7 Deferred PySpark Family Admission

## Decision

v7 admits all three transformation families deferred at kickoff: binary encoding, schema-carrying JSON/CSV conversion,
and deterministic mode. Admission is staged because each family needs a public type or a multi-stage relational recipe;
none may be exposed as an untyped pass-through to PySpark.

## Binary Values and Encoding

Add a public immutable Binary field type and `binary(...)` factory. Its declared nullable shape maps directly to Spark
binary values. `base64`, `unbase64`, `encode`, and `decode` use typed expressions with literal charset options. Encoding
returns Binary; decoding returns nullable String because malformed bytes or unsupported decoding behavior must not be
claimed non-null. The contract records whether an invalid base64 or decode sequence is null-producing on each supported
target and rejects a profile disagreement before release.

Binary is data, not an action or a file abstraction. It does not admit arbitrary codecs, file reads, or driver-side byte
conversion into the DSL.

## Schema-Carrying JSON and CSV

`from_json(value, as_=Schema, options=...)` returns an exact typed Struct. `to_json(value, options=...)` accepts a
typed Struct and returns nullable String. `from_csv(value, as_=Schema, options=...)` returns an exact typed Struct, and
`to_csv(value, options=...)` accepts an exact typed Struct. Options are a small immutable, documented literal-value
record, not a free-form dictionary, so online evaluation and generated rendering have one normalized representation.

The first option set contains only delimiter, quote, escape, null value, date format, timestamp format, and permissive
parse mode. Unknown options, dynamic option values, map outputs, variant values, and schema inference are rejected.
Parse failures are represented as nullable results or nullable fields according to the selected Spark-compatible mode;
the specification and live target matrix must state the exact behavior before support is claimed.

## Deterministic Mode

Expose the PySpark spelling `mode(value, deterministic=False)` as an aggregate expression after `group_by(...)`.
For example, a step calls `group_by(order.customer_id)` and returns
`CustomerSummary(customer_id=order.customer_id, preferred_category=mode(order.category, deterministic=True))`.
The normal Structure aggregate placement and declared output Schema remain unchanged; callers do not learn a separate
`select_mode` API.

`deterministic=False` is the PySpark-compatible default and may return any tied most-frequent value. The Apache Spark
API documents `mode` as available since Spark 3.4 and the deterministic argument as added in 4.0. [Apache Spark mode
reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.mode.html)

`deterministic=True` lowers behind the same public spelling to a typed portable aggregate: collect non-null candidates
inside the existing grouping keys, compute candidate counts with Spark higher-order array expressions, select the
greatest count, then select the lowest tied candidate through Spark's ascending order. This lowering requires an
orderable candidate type and is visible in the recipe, online evaluator, and generated code. It returns the same result
on PySpark 3.5 and 4.0 for the shared target types. `mode(...)` is batch-only initially: no global/unbounded streaming
mode and no implicit stateful composition.

## Required Evidence

Each family requires capability records, symbolic capture, recipe, online evaluator, generated renderer, explain and
traceability behavior, diagnostic tests, public reference, online/generated parity, and live PySpark 3.5/4.0 evidence.
The catalog must split each admitted family from any residual excluded form rather than leaving a mixed deferred row.

The normative behavior is [V7 Binary Values and Encoding](../specifications/V7BinaryEncoding.md),
[V7 Schema-Carrying JSON and CSV Conversion](../specifications/V7SchemaCarryingParsing.md), and
[V7 Grouped Mode](../specifications/V7DeterministicMode.md).
