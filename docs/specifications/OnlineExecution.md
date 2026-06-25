# Online Execution

## Purpose

Online execution is the default v1 way to run Structure transforms. A user depends on the Structure library, supplies an
existing Spark session, constructs a transform invocation with input DataFrames, and runs it through a
`StructureSession`. The user does not need to commit generated PySpark code to their repository.

Generated PySpark remains available for provenance, review, debugging, CI diff checks, and projects that deliberately
choose generated execution.

## Public API

The default runtime shape is:

```python
from structure import StructureSession
from orders.transforms.order import EnrichOrders

session = StructureSession(spark=spark, ctx=ctx)

result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
    products=products_df,
).run(session)

enriched_df = result.published
```

The transform instance is a deferred invocation. Its constructor stores named input DataFrames and performs no Spark
work. Calling `run(session)` delegates to the session:

```python
transform = EnrichOrders(orders=orders_df, customers=customers_df)
result = transform.run(session)
```

The session can also be used directly:

```python
result = session.run(EnrichOrders(orders=orders_df, customers=customers_df))
```

When caller code needs the Spark schema after online execution, keep the transform invocation:

```python
transform = EnrichOrders(orders=orders_df, customers=customers_df)
result = transform.run(session)

output_schema = transform.schemas.output
```

`transform.schemas.output` is a materialized PySpark `StructType` equivalent to the generated `*_SCHEMA` constant for
the final output schema. The schema is available in online mode without requiring generated files to exist.

`run(session)` returns a read-only `TransformResult` for both single-output and multi-output transforms. Results expose
declared output names such as `result.published`, `result.accepted`, and `result["rejected"]`. There is no automatic
`df` alias; `df` is present only when a field-declared output is explicitly named `df`.

Online execution evaluates transform methods in source order while preserving independent lane frames. When schemas are
unambiguous, methods consume and update inferred lanes without method-level selectors. Method-level `input=` selects
original inputs or existing lanes, `output=` names intermediate lanes or final outputs, and both options accept ordered
lists. If a lane shares an input name, the lane shadows that original input in method-level `input=`.

## Configuration

Online execution is the default:

```toml
[tool.structure]
execution_mode = "online"
target_backend = "pyspark"
target_pyspark = ">=3.5,<4.1"
```

Allowed execution modes:

```text
online
generated
```

`online` runs transforms through a runtime runner that consumes compiler IR and live PySpark objects. `generated`
delegates to checked-in generated PySpark classes.

`target_backend` and `target_pyspark` remain backend selection inputs. In v1 the only supported backend is `pyspark`.
Future backends should be selected by the session, not by changing transform constructors. Backend support is checked
through `docs/specifications/BackendCapabilities.md`, so online execution and generated PySpark share the same target
capability decisions.

## Session Responsibilities

`StructureSession` owns runtime knowledge:

- Spark session supplied by the caller;
- optional `ctx` passed to hooks;
- resolved Structure configuration;
- selected execution mode;
- selected target backend and PySpark target range;
- runtime runner delegation;
- materializing Spark `StructType` schemas for online execution;
- optional in-memory compiled-plan cache.

`StructureSession` must not start Spark, stop Spark, mutate Spark configuration silently, read or write streaming
queries, or own orchestration concerns such as Airflow DAGs, triggers, checkpoints, or output sinks.

## Execution Modes

In online mode, the session delegates to `OnlinePySparkRunner`. The runner compiles the transform class to
`TransformPlan` IR on demand, lowers that IR through the shared PySpark execution semantic contract, then interprets
the resulting recipes with PySpark DataFrame and Column APIs. It must not write generated files and must not execute
generated Python source text.

The online runner must also materialize the transform's Spark schemas from the checked schema model and expose them on
the transform invocation. This gives caller code the same shape contract that generated schema modules provide in
generated-code workflows.

In generated mode, the session delegates to `GeneratedPySparkRunner`. The runner imports the generated PySpark class,
instantiates it with `spark=session.spark` and `ctx=session.ctx`, and calls `run(...)` with the transform invocation's
stored inputs.

If generated mode cannot import the generated class, Structure must fail with a diagnostic that suggests running
`structure compile`, making the generated source root importable, or switching to `execution_mode = "online"`.

## Execution Order

Online execution must preserve generated-code semantics:

1. Validate declared input DataFrames.
2. Create a read-only hook input namespace only when at least one hook declares `pass_inputs=True`.
3. Execute subtransforms in source order.
4. Run `@before` hooks before the compiled operations for their target step.
5. Lower shared filters and joins, then materialize every ordered result projection.
6. Run each `@after` hook against its selected result DataFrame.
7. Validate intermediate schemas according to project, class, and method policy.
8. Apply hook `schema_mode` and `project_output` rules.
9. Validate every output DataFrame.
10. Return a read-only `TransformResult`.

Online and generated execution must agree on hook order, validation placement, expression lowering, join aliasing,
projection shape, schema projection, result shape, and performance guardrails.

For a multi-result step, joins and filters execute once. Each result projection starts from that shared DataFrame and is
stored under its output lane name.

Those shared semantics are owned by `docs/specifications/ExecutionSemanticContract.md`. Online execution owns live
DataFrame binding and runtime hook invocation; it must not independently choose aliases, validation placement,
expression mapping, or literal typing when a shared PySpark recipe already defines them.

## Transform Input Binding

`Transform.__init__(**inputs)` stores DataFrame inputs by declared Structure input name. Positional arguments are not
allowed. Unknown input names are errors. Missing declared inputs must be reported no later than `run(session)`.

For v1, custom transform construction parameters should not be mixed into the transform constructor. Runtime context
belongs in `StructureSession(ctx=...)`. Future explicit APIs may add richer parameter binding if a concrete use case
requires it.

`run` is reserved for online execution. A public schema-returning subtransform named `run` must fail with a structured
diagnostic that asks the user to rename it.

## Compiler Boundary

`structure check`, `structure compile`, and generated-file diff checks remain Spark-free. They must not require
PySpark, Java, SparkSession, Spark startup, or a Spark cluster.

Online execution is a runtime boundary and may import PySpark. Runtime tests for online execution may require a local
Spark runtime.

## Streaming Compatibility

Online execution does not change the v1 streaming contract. A transform is streaming-compatible when its compiled
operations are valid for the caller's streaming DataFrame shape. The caller still owns `readStream`, `writeStream`,
triggers, checkpoints, output modes, and query lifecycle.

## Diagnostics

Diagnostics must include:

- diagnostic code;
- transform class;
- execution mode;
- target backend;
- input name, hook name, subtransform, or field when relevant;
- problem;
- suggested fix;
- link to this specification or `docs/Configuration.md`.

Example:

```text
RuntimeError GEN-E0902: Generated transform is not importable

Transform:
  orders.transforms.order.EnrichOrders

Execution mode:
  generated

Problem:
  Structure could not import the generated PySpark class for this transform.

Use:
  Run `structure compile`, ensure the generated source root is importable, or set `execution_mode = "online"`.

See docs/specifications/OnlineExecution.md
```

## Acceptance Criteria

The implementation is complete when tests prove:

- default config resolves `execution_mode = "online"`;
- invalid execution modes fail with allowed values and a configuration docs link;
- `EnrichOrders(orders=..., customers=...).run(session)` runs a projection-only transform;
- constructing a transform invocation performs no Spark action;
- unknown constructor input names fail clearly;
- missing declared inputs fail clearly no later than run time;
- a public subtransform named `run` fails with a reserved-name diagnostic;
- online and generated execution produce equivalent results for projection, filtering, expression helpers, joins,
  hooks, `pass_inputs=True`, validation, `schema_mode`, and `project_output`;
- online execution consumes the shared PySpark execution recipes defined by
  `docs/specifications/ExecutionSemanticContract.md`;
- online execution exposes the final output Spark schema after `run(session)` without requiring generated files;
- generated mode delegates through generated classes using the same builder-style transform invocation;
- missing generated code in generated mode suggests `structure compile` or online mode;
- compiler commands remain Spark-free.
