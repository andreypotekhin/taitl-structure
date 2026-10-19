# V11 Arrow UDFs

Arrow UDFs let a Transform apply Python logic to batches of values represented by PyArrow arrays. This design keeps the callback in Spark's Python worker and exposes its result through typed Structure expressions. It follows the native Spark 4.1 `arrow_udf` contract.

## Declaration

Declare callbacks with `@special(type="arrow_udf", return_type=...)`. The SQL result type is required and must be a concrete Structure type. Input SQL types come from the expressions at the call site. Supported modes are `scalar` (default), `scalar_iter`, and `grouped_agg`; users select the mode explicitly rather than relying on annotations.

`nullable=True` is the default Structure declaration. `deterministic` is optional: omission retains Spark's deterministic default, `True` states that default, and `False` applies native `asNondeterministic()`. These declarations do not change Spark's physical result validation.

Scalar callbacks receive one Arrow array per input column and return one array for the batch. Iterator callbacks receive a single iterator whose items contain one array or tuples of arrays; they yield result arrays covering the same total row count. Grouped callbacks receive arrays and return one native-supported scalar for each group or window.

Scalar and grouped calls preserve Python positional, named, and mixed argument binding. Python signatures govern positional-only and keyword-only parameters and defaults. Iterator calls take column expressions positionally and always call the worker function with one iterator parameter. An all-keyword-only scalar callback is rejected because Spark's Arrow UDF registration does not accept a callback with zero positional parameters. Grouped callbacks may take no input columns. The keyword `over` is reserved for a typed Structure window and is accepted only for `grouped_agg`.

## Runtime boundaries

Callbacks are opaque Python boundaries. Structure checks callable shape, declared options, SQL type, and call binding while compiling; it does not execute the body or inspect callback results. Spark owns output shape, cardinality, conversion, nullability, and worker errors. PyArrow is optional in the compiler process; callback-local imports are recommended.

Support is gated to the exact `>=4.1,<4.2` profile and admitted independently for ordinary and Spark Connect. Scalar and iterator modes can be used in ordinary streaming after multiple-batch and checkpoint-restart parity. Grouped aggregation and analytic windows over streaming inputs are rejected during compilation because the pinned native Spark runtime rejects those compositions. Iterator-local Python state is not checkpointed.

Grouped Arrow metrics cannot be mixed with built-in aggregate metrics in the same grouped query because native Spark rejects that placement. Calculate the metrics in separate grouped steps and join their results when needed. Grouped callbacks can require substantial memory because Spark passes group data to Python.

## Evidence required

Each mode is promoted only after native, Structure online, and generated execution agree on rows and physical schema for the exact backend profile. Ordinary and Connect batch evidence is independent. Streaming support additionally requires processed batches and restart from the same durable checkpoint. Startup alone is not evidence. Generated code must use public PySpark APIs and cannot access classic Spark internals.
