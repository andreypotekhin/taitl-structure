# UDF Background

## Why UDFs are a boundary

Structure normally turns a transform into typed Spark `DataFrame` and `Column` operations. Spark can inspect those
operations, optimize their order, push filters toward data sources, and choose execution strategies. A Python UDF asks
Spark to call user code instead. Spark knows the declared input expressions and result type, but it cannot inspect the
callback's logic or optimize operations inside it.

That boundary is intentional. Unsupported Python must not be guessed into a UDF: the author opts in with
`@special(type="udf")` or `@special(type="arrow_udf", ...)`. A missing native expression should otherwise fail during
compilation. If a native PySpark operation is needed outside the admitted DSL, use a declared `@raw` hook or another
caller-owned integration boundary.

## Two callback shapes

The scalar Python UDF receives one Python value per row and returns one Python value. It is useful for small row-local
calculations that have no suitable native expression. Its declared result type and nullability are part of the source
contract because Structure cannot inspect the callback.

Arrow UDFs pass Arrow arrays (or an iterator of Arrow batches) to the callback. They can reduce Python call overhead
for scalar batch work and can express grouped or analytic calculations. Arrow does not remove the Python boundary:
Spark still needs to transfer values to worker processes, invoke the callback, and convert results. Grouped callbacks
can retain an entire group in worker memory. A symbolic Spark expression remains preferable when it can express the
same logic.

## Compiler and runtime responsibilities

During compilation, Structure validates decorator options, callable shape, concrete return type, expression argument
binding, profile capability, and restricted compositions. It records the callback as optimizer-opaque and includes its
source owner in traceability. It does not invoke the callback, import PyArrow to inspect it, infer its result, or inspect
returned objects.

At execution, Spark owns callback invocation, Arrow conversion, result cardinality, SQL conversion, null behavior, and
worker exceptions. These errors remain native Spark errors. `nullable` is a declaration used by Structure's checks; it
does not rewrite Spark's physical UDF nullability. `deterministic=False` selects the native nondeterministic UDF form;
omission preserves Spark's default.

## Generated code and dependency ownership

Generated transforms preserve the same callback contract. By default, generated code delegates to the declaring source
transform for source-backed callback methods. The explicit `embed_udfs` option controls whether eligible UDF bodies are
copied into generated modules; embedding does not make the callback optimizer-visible or change its validation. The
user remains responsible for callback dependencies on the driver and workers. For Arrow callbacks, importing PyArrow
inside the callback keeps compilation independent of that package.

## Streaming and memory

Streaming admission depends on both callback mode and target. Ordinary PySpark scalar Python UDFs and Arrow scalar or
scalar-iterator UDFs have separate tested streaming contracts; Arrow scalar and iterator restart evidence covers
processed data and recovery from the same durable checkpoint. Iterator-local Python state is not restored after a
restart. Grouped Arrow aggregates and analytic windows are batch-only. Spark Connect streaming is outside the Arrow
UDF support claim. Consult the [UDF reference](../reference/UDF.ref.md#compatibility) before relying on a target or
mode combination.

Grouped Arrow callbacks may require Spark to materialize all values for a group in Python worker memory. Keep groups
bounded where possible and avoid treating Arrow batching as a guarantee that grouped input is bounded.
