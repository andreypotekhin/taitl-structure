# V11 Arrow UDF specification

## Public declaration

The public decorator is `@special(type="arrow_udf", return_type=...)`. It accepts `function_type="scalar" | "scalar_iter" | "grouped_agg"` with default `"scalar"`, `nullable=True`, and optional `deterministic: bool`. `return_type` is required and must be a concrete Structure type instance or type class. Unknown options, invalid option types, non-callables, coroutine functions, and async generators fail before Spark execution.

The callback body is not executed during compilation. PyArrow is not a compiler dependency. Input types come from typed call expressions. The decorator does not infer a mode from annotations.

## Call contract

Scalar callbacks receive one Arrow array per expression argument and return one Arrow array for each native batch. Scalar calls support positional, keyword, and mixed expression arguments bound against the Python signature. Python defaults are honored without creating additional column arguments. An all-keyword-only callback is rejected because native scalar Arrow UDF registration requires at least one positional parameter.

Iterator callbacks receive one iterator argument. Each item is an array for a single input or a tuple of arrays for multiple inputs. Calls accept one or more positional input expressions only. The callback yields Arrow arrays whose combined row count matches the inputs; Spark performs native checks.

Grouped callbacks receive one array per expression argument and return one native-supported scalar per group. Zero-input callbacks are allowed. Positional, keyword, and mixed expression calls follow Python signature binding. The reserved keyword `over` accepts a Structure `WindowSpec` only for grouped mode. Calls without `over` become grouped aggregates; calls with it become analytic aggregate expressions.

`nullable` is an authoring declaration used by Structure validation. It does not override Spark's native physical nullability. Omitted `deterministic` leaves Spark's default unchanged; `True` leaves that behavior explicit; `False` calls public `.asNondeterministic()`. Structure's optimizer treats every callback as opaque regardless of this flag.

## Composition and capabilities

The capability requirements are `expression.arrow_udf_scalar`, `expression.arrow_udf_scalar_iter`, `aggregate.arrow_udf_grouped_agg`, and `window.arrow_udf_grouped_agg`. Each is supported only for exact `>=4.1,<4.2`, and ordinary and Spark Connect are admitted independently after parity evidence.

Grouped Arrow metrics cannot share one aggregate query with built-in aggregate metrics. Structure reports this composition during compilation and recommends separate grouped steps. Grouped Arrow streaming and grouped analytic windows over streaming inputs fail during compilation with `STREAM-E0801`. Ordinary scalar and iterator streaming require processed-data and durable-checkpoint restart evidence. No iterator-local callback state survives restart by contract.

## Native result behavior

Structure validates declaration options and binds expression arguments, but never calls the callback body or adds result wrappers. Spark controls output values, cardinality, SQL conversion, null handling, and worker exceptions. Errors from incompatible return objects or lengths remain native Spark errors. Generated code calls public PySpark APIs only and must not access `_jdf`, `_jvm`, RDDs, SparkContext, or trigger hidden actions.

The public default compatibility profile remains `>=3.5,<4.1`; Arrow UDFs are unavailable there and on profiles outside `>=4.1,<4.2`.
