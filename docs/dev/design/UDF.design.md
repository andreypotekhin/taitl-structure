# UDF Design

## Purpose

UDFs are an explicit boundary for calculations that have no suitable typed Spark expression. They let Spark execute
user Python while making the cost visible: Spark SQL cannot inspect or optimize the callback body, and execution now
depends on Python worker serialization, dependencies, and callback behavior. Structure must never turn unsupported
Python into a UDF implicitly.

The [UDF specification](../specifications/UDF.spec.md) defines the normative declaration and runtime contract. The
[UDF reference](../../reference/UDF.ref.md) provides public examples; the [background](../../background/UDF.back.md)
explains when this boundary is useful.

## One explicit boundary, two execution shapes

Scalar Python UDFs receive Python values one row at a time. Arrow UDFs receive Arrow arrays or batches and can reduce
per-row Python call overhead. They share the same design boundary—opaque Python with a declared SQL result type—but
their callback signatures, result shape, supported modes, and runtime costs differ. They therefore have distinct
declarations and capability rows rather than being inferred from annotations or selected as an optimization.

Use a built-in typed expression whenever it describes the operation. Use `@special(type="udf", ...)` for a row-local
Python callback, or `@special(type="arrow_udf", ...)` for a supported Arrow callback. Relation-level Python,
DataFrame access, arbitrary callback registration, Pandas UDFs, and table-valued UDFs remain outside these contracts;
use a caller-owned integration boundary for those APIs.

## Declaration and type ownership

The author declares a concrete Structure return type because callback code is opaque to the symbolic compiler. Scalar
Python UDFs also require an explicit `nullable` Boolean. Arrow UDFs default `nullable=True`; optional
`deterministic` metadata preserves Spark's native deterministic default when omitted and maps `False` to the public
`asNondeterministic()` operation.

These declarations support compile-time validation and traceability. They do not change Spark's physical nullability,
result conversion, or validation of callback output. Structure validates the callable, options, and Python argument
binding, but it does not run the callback or inspect returned values. Spark remains the authority for output shape,
cardinality, conversion, null behavior, and worker exceptions. This keeps compilation free of callback side effects and
avoids inventing a second result-validation contract.

## Python call binding

Scalar Python and Arrow scalar/grouped callbacks follow Python's normal positional and named argument rules. Expression
arguments bind against the callback signature, including defaults and positional-only or keyword-only parameters. This
preserves ordinary Python calling semantics: a caller chooses positional or named binding for each argument according
to the function signature, and Structure rejects invalid combinations before Spark execution.

Arrow iterator mode has a different native callback contract. Call-site expressions are positional, while Spark calls
the worker function with one iterator argument. Each iterator item contains the input Arrow array or tuple of arrays.
The callback yields result arrays; Spark checks the aggregate row cardinality. Iterator-local Python state is not
checkpoint state and is not restored after streaming restart.

Arrow grouped aggregation returns a Spark-supported scalar for each group. A typed `over` window is reserved for this
mode. Grouped callbacks may have no input expressions, but Spark does not allow a grouped Arrow metric to share the
same grouped query with built-in aggregate metrics. Structure reports that composition and directs users to separate
the aggregate steps. Group size is not bounded by this API, so large groups can require substantial Python worker
memory.

## Runtime and capability boundaries

The scalar Python UDF capability is admitted only where the selected PySpark profile exposes the public `udf` API.
Arrow UDF modes are individually gated to exact PySpark `>=4.1,<4.2` ordinary and Spark Connect profiles; a profile or
backend is admitted only after its own parity evidence. Ordinary streaming admits Arrow scalar and scalar-iterator
modes with processed-batch and checkpoint-restart evidence. Grouped aggregation and analytic windows over streaming
inputs are rejected, and Connect streaming is not claimed. PyArrow is needed in callback workers, not in the compiler
process merely to declare an Arrow UDF.

Generated code uses public PySpark APIs and preserves callback ownership and mode. It must not access classic Spark
internals, convert through RDDs, trigger actions, or reconstruct callback behavior from runtime values. Generated
modules normally delegate to the source transform instance; embedding remains governed by the separate `embed_udfs`
dependency and standalone restrictions. UDF use remains visible in explain/traceability, and scalar UDFs retain the
existing `DSL-W0403` warning unless the author disables warnings.

## Design outcomes

- Keep UDF creation explicit. Missing native-expression support is not permission for an automatic Python fallback.
- Keep result types declared and concrete; do not infer SQL types from Python annotations or execute callbacks during
  compilation.
- Follow native Spark behavior for callback results and failures. Structure validates declarations and valid Python
  call binding, without strict runtime return-object validation.
- Keep determinism optional for Arrow UDFs so omission retains Spark's default behavior.
- Keep each Arrow function mode explicit and distinct because it changes the worker signature, result shape, and
  streaming contract.
- Treat `nullable` as a Structure declaration and do not claim it overrides Spark's runtime schema behavior.
- Admit backend/profile/streaming combinations independently; successful registration or startup is not parity
  evidence.

## Evidence expected

For each admitted callback mode and target, compare native PySpark, Structure online execution, and generated execution
for values and physical schema. Spark-free tests cover declarations, type requirements, options, binding, capability
rejection, generated spelling, traceability, and scans for classic-only access. Streaming claims also require processed
batches and restart from the same durable checkpoint. Tests should preserve native Spark callback errors and confirm
that no unsupported Python operation is implicitly lowered to a UDF.
