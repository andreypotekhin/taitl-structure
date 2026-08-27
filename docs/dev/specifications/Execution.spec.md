# Execution

## Purpose

Execution is the default v1 way to run Structure transforms. A user depends on the Structure library, supplies an
existing Spark session, constructs a transform invocation with input DataFrames, and runs it through a
`StructureSession`. The user does not need to commit generated PySpark code to their repository.

Generated PySpark remains available for provenance, review, debugging, CI diff checks, and projects that deliberately
choose generated-code execution.

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

enriched_df = result.enriched
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

When caller code needs the Spark schema after execution, read it from the transform result:

```python
transform = EnrichOrders(orders=orders_df, customers=customers_df)
result = transform.run(session)

output_schema = result.schema.enriched
same_schema = result.schema["enriched"]
```

`result.schema.enriched` is a materialized PySpark `StructType` equivalent to the generated `*_SCHEMA` constant for
the `enriched` output. The schema is available in direct execution without requiring generated files to exist.

`run(session)` returns a read-only `TransformResult` for both single-output and multi-output transforms. Results expose
declared output names such as `result.published`, `result.accepted`, and `result["rejected"]`. Output schemas expose
the same names through `result.schema`, such as `result.schema.published` and `result.schema["rejected"]`. There is no
automatic `df` alias; `df` is present only when a field-declared output is explicitly named `df`.

For a composed transform, declared outputs of child transforms are available through a recursive stage namespace when
`allow_stage_outputs` is enabled (the default): `result.vectorized.query_embeddings` and
`result.stages["vectorized"]["query_embeddings"]` are equivalent lookups. Nested compositions retain nested stage
paths. Stage namespaces expose only child `output(...)` declarations; lanes and raw-hook frames remain private. The
stage namespace is omitted when `allow_stage_outputs=False`, while final outputs remain available normally.

Execution evaluates transform methods in source order while preserving independent lane frames. When schemas are
unambiguous, methods consume and update inferred lanes without method-level selectors. Method-level `input=` selects
original inputs, existing lanes, or already-produced outputs; `output=` names intermediate lanes or final outputs, and
both options accept ordered lists. If a lane shares an input name, the lane shadows that original input. Reusing an
output preserves the earlier branch for return and for other consumers, as it would in ordinary lazy PySpark code.

## Configuration

Execution is the default:

```toml
[tool.structure]
execution_mode = "online"
target_backend = "pyspark"
target_profile = ">=3.5,<4.1"
```

Allowed execution modes:

```text
online
generated
```

The stable `execution_mode` values are implementation/configuration names: `online` selects direct execution through a
runtime runner that consumes compiler IR and live PySpark objects; `generated` selects generated-code execution through
checked-in generated PySpark classes. The values remain compatible, while public prose uses execution and
generated-code execution.

`target_backend` and `target_profile` remain backend selection inputs. In v1 the only supported backend is `pyspark`.
Future backends should be selected by the session, not by changing transform constructors. Backend support is checked
against the session's resolved `StructureConfig` through [BackendCapabilities.spec.md](BackendCapabilities.spec.md), so execution
and generated-code execution share the same target capability decisions.

Python users may pass a resolved config to the runtime session:

```python
from structure import StructureConfig, StructureSession

config = StructureConfig.resolve(project_root=".", execution_mode="generated")
session = StructureSession(spark=spark, config=config)
```

## Session Responsibilities

`StructureSession` owns runtime knowledge:

- Spark session supplied by the caller;
- optional `ctx` passed to hooks;
- resolved Structure configuration;
- selected execution mode;
- selected target backend and PySpark target range;
- runtime runner delegation;
- materializing Spark `StructType` schemas for execution;
- a session-owned in-memory compiled-artifact pool.

`StructureSession` must not start Spark, stop Spark, mutate Spark configuration silently, read or write streaming
queries, or own orchestration concerns such as Airflow DAGs, triggers, checkpoints, or output sinks.

The session compiles a transform lazily on its first compatible invocation and reuses the resulting artifact for later
invocations. `Transform.compile(...)` remains available for early diagnostics; callers load its result with
`session.load(artifact)`. Sessions are isolated by default, while applications may deliberately share a
`CompiledArtifactPool` between sessions. Clearing a pool releases derived compiler state without affecting Spark or a
currently executing invocation.

## Execution Modes

In direct execution (`execution_mode = "online"`), the session delegates to `OnlinePySparkRunner`. The session obtains a checked compiled artifact from
its pool, compiling it on a cache miss. The runner interprets the artifact's shared PySpark recipes with live PySpark
DataFrame and Column APIs. It must not write generated files and must not execute generated Python source text.

The execution runner must also materialize the transform's Spark schemas from the checked schema model and expose them on
the transform invocation. This gives caller code the same shape contract that generated schema modules provide in
generated-code workflows.

In generated-code execution (`execution_mode = "generated"`), the session delegates to `GeneratedPySparkRunner`. The
runner imports the generated PySpark class,
instantiates it with `spark=session.spark` and `ctx=session.ctx`, and calls `run(...)` with the transform invocation's
stored inputs.

If generated mode cannot import the generated class, Structure must fail with a diagnostic that suggests running
`structure compile`, making the generated source root importable, or switching to `execution_mode = "online"`.

## Execution Order

Execution must preserve generated-code semantics:

1. Validate declared input DataFrames.
2. Create a read-only hook input namespace only when at least one hook declares `pass_inputs=True`.
3. Execute step methods and `@raw` hooks in Transform class declaration order.
4. For each step, lower shared filters and joins, then materialize every ordered result projection.
5. Invoke each raw hook against its selected lane DataFrame at its source-order boundary.
6. Validate intermediate schemas according to project, class, and method policy.
7. Apply hook `schema_mode` and `project_output` rules at the hook boundary.
8. Validate every output DataFrame.
9. Return a read-only `TransformResult`.

Execution and generated-code execution must agree on hook order, validation placement, expression lowering, join aliasing,
watermark placement, projection shape, schema projection, result shape, and performance guardrails. A watermark on the
current relation is applied at its ordered recipe position. A watermark declared for a joined input is applied to that
input before Structure aliases, hints, or joins it.

For a multi-result step, joins and filters execute once. Each result projection starts from that shared DataFrame and is
stored under its output lane name.

Those shared semantics are owned by [ExecutionSemanticContract.spec.md](ExecutionSemanticContract.spec.md). Execution owns live
DataFrame binding and runtime hook invocation; it must not independently choose aliases, validation placement,
expression mapping, or literal typing when a shared PySpark recipe already defines them.

## Transform Input Binding

`Transform.__init__(**inputs)` stores DataFrame inputs by declared Structure input name. Positional arguments are not
allowed. Unknown input names are errors. Missing declared inputs must be reported no later than `run(session)`.

For v1, custom transform construction parameters should not be mixed into the transform constructor. Runtime context
belongs in `StructureSession(ctx=...)`. Future explicit APIs may add richer parameter binding if a concrete use case
requires it.

`run` is reserved for execution. A public schema-returning step method named `run` must fail with a structured
diagnostic that asks the user to rename it.

## Compiler Boundary

`structure check`, `structure compile`, and generated-file diff checks remain Spark-free. They must not require
PySpark, Java, SparkSession, Spark startup, or a Spark cluster.

Execution is a runtime boundary and may import PySpark. Runtime tests for execution may require a local Spark runtime.

## Streaming Compatibility

Execution does not change the v1/v2 streaming compatibility contract. A transform is streaming-compatible when
its compiled operations are valid for the caller's streaming DataFrame shape. The caller owns `readStream`,
`writeStream`, triggers, checkpoints, output modes, and query lifecycle.

## Diagnostics

Diagnostics must include:

- diagnostic code;
- transform class;
- execution mode;
- target backend;
- input name, hook name, step method, or field when relevant;
- problem;
- suggested fix;
- link to this specification or [Configuration.md](../../Configuration.md).

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

See docs/dev/specifications/Execution.spec.md
```

## Acceptance Criteria

The implementation is complete when tests prove:

- default config resolves `execution_mode = "online"`;
- invalid execution modes fail with allowed values and a configuration docs link;
- `EnrichOrders(orders=..., customers=...).run(session)` runs a projection-only transform;
- constructing a transform invocation performs no Spark action;
- unknown constructor input names fail clearly;
- missing declared inputs fail clearly no later than run time;
- a public step method named `run` fails with a reserved-name diagnostic;
- execution and generated-code execution produce equivalent results for projection, filtering, expression helpers, joins,
  hooks, `pass_inputs=True`, validation, `schema_mode`, and `project_output`;
- execution consumes the shared PySpark execution recipes defined by
  [ExecutionSemanticContract.spec.md](ExecutionSemanticContract.spec.md);
- execution exposes the final output Spark schema after `run(session)` without requiring generated files;
- generated mode delegates through generated classes using the same builder-style transform invocation;
- missing generated code in generated-code execution suggests `structure compile` or direct execution;
- compiler commands remain Spark-free.
