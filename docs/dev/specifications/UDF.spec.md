# UDF Specification

This document is the normative contract for Structure's explicit scalar Python and Arrow UDF declarations. A UDF is an
opt-in Python callback executed by Spark. It is opaque to Structure's expression compiler and to Spark SQL optimization.
Unsupported Python never becomes a UDF implicitly.

## Scalar Python UDF

Declare a row-local callback with `@special(type="udf", return_type=..., nullable=...)`. `return_type` must be a
concrete type supplied by `structure.plugin.pyspark`; `nullable` must be a Boolean and is required. The callback has an
ordinary Python signature, receives one value per expression argument, and returns one Python value per row. It must
not receive a DataFrame, Spark session, execution context, or iterator.

Structure binds call-site expressions using the callback signature and records the declaration as an opaque compiler
boundary. It does not execute the callback or infer result behavior. The selected PySpark runtime owns registration,
conversion, null handling, and worker errors. The existing warning policy emits `DSL-W0403` by default;
`warn_on_udfs=False` suppresses that warning only.

Scalar Python UDF capabilities are admitted for target profiles exposing the public PySpark `udf` API. Ordinary and
Spark Connect use their public DataFrame/Column APIs; exact capability rows remain the authority for each profile.
Streaming support follows the tested ordinary-PySpark contract. Unsupported profiles fail capability analysis rather
than silently generating a fallback.

## Arrow UDF declaration and modes

Declare an Arrow callback with `@special(type="arrow_udf", return_type=...)`. The return type is required and must be a
concrete Structure type instance or type class. Accepted options are `function_type="scalar" | "scalar_iter" |
"grouped_agg"` (default `"scalar"`), `nullable: bool = True`, and optional `deterministic: bool`. Unknown options,
invalid option types, non-callables, coroutine functions, and async generators fail before Spark execution.

Input SQL types come from the expressions passed at the call site. The callback body does not run during compilation;
PyArrow is not a compiler dependency. Structure validates the declaration and call binding without inspecting the
callback's returned object.

### Scalar mode

Scalar callbacks receive one Arrow array for each expression argument and return one Arrow array per native input batch.
Positional, named, and mixed expression calls follow Python signature binding, including defaults, positional-only
parameters, and keyword-only parameters. An all-keyword-only callback is rejected because native Arrow UDF registration
requires a positional callback parameter.

### Scalar iterator mode

Iterator callbacks receive one iterator parameter. Each item is an Arrow array for one input column or a tuple of
arrays for multiple columns. Calls supply input expressions positionally. The callback yields Arrow result arrays whose
combined row count matches the input; Spark performs native cardinality checks. Iterator-local state is not restored
across a streaming restart.

### Grouped aggregate mode

Grouped callbacks receive one Arrow array per expression argument and return one Spark-supported scalar for each group.
Zero-input callbacks are allowed. Expression calls follow Python signature binding. The reserved keyword `over` accepts a
Structure `WindowSpec`: without `over`, the expression is a grouped aggregate; with it, the expression is an analytic
window aggregate. `over` is reserved and cannot be a callback parameter.

Grouped Arrow metrics cannot share one grouped query with built-in aggregate metrics. Structure diagnoses this
composition and recommends separate grouped steps. Grouped data may be materialized in Python worker memory; the API
does not promise bounded group size.

## Nullability, determinism, and native result behavior

Arrow `nullable` is a Boolean author declaration used by Structure validation. It does not override Spark's physical
nullability. Omitting `deterministic` retains Spark's default; `True` leaves that behavior explicit; `False` applies the
public `.asNondeterministic()` method. Every callback remains optimizer-opaque regardless of determinism.

Spark owns callback invocation, result shape and cardinality, SQL conversion, null handling, and worker exceptions.
Structure adds no result wrapper and performs no strict return-object validation. Incompatible values, lengths, or
result objects surface as native Spark errors.

## Capability and streaming gates

The Arrow capabilities are `expression.arrow_udf_scalar`, `expression.arrow_udf_scalar_iter`,
`aggregate.arrow_udf_grouped_agg`, and `window.arrow_udf_grouped_agg`. Each is supported only for exact
`>=4.1,<4.2` profiles and ordinary and Spark Connect variants are admitted independently after parity evidence. The
default profile is `>=3.5,<4.2`; Arrow operations remain gated to the exact supported range.

Ordinary and Connect batch support covers all three modes. Ordinary streaming supports scalar and scalar-iterator
modes after processed-data and durable-checkpoint restart evidence. Grouped aggregates and analytic windows over
streaming inputs fail compilation. Connect streaming is outside this feature's contract. No iterator-local callback
state is included in checkpoint state.

## Generated code and traceability

Generated code must use public PySpark APIs and retain the declared result type, callback source owner, and mode. It
must not access classic Spark internals (`_jdf`, `_jvm`, RDDs, or SparkContext), trigger an action, or reconstruct a
callback from runtime values. By default, generated code delegates to the source transform instance. The existing
`embed_udfs` option may embed eligible source-backed callback bodies under its independent dependency and standalone
restrictions.

Traceability identifies each callback as an opaque UDF boundary. Diagnostics for missing result types, invalid options,
binding errors, unsupported capabilities, forbidden compositions, and unavailable source ownership name the relevant
declaration and a corrective path. Prefer a typed native expression for equivalent logic; use `@raw` only when the
whole operation requires caller-owned DataFrame behavior.

## Acceptance

Every supported mode must have Spark-free declaration, type, binding, capability, diagnostics, and generated-source
checks. For each claimed backend and runtime profile, native PySpark, Structure online execution, and generated
execution must agree on values and physical schema. Streaming claims additionally require processed batches and
restart from the same durable checkpoint. Tests must verify callback worker failures remain native, and that no
unsupported Python operation is implicitly lowered to a UDF.
