# Execution

Execution is the default v1 way to run Structure transforms. A user depends on the Structure library, supplies an
existing Spark session, constructs a transform invocation with input DataFrames, and runs it through a
`StructureSession`. The user does not need to commit generated PySpark code to their repository.

Generated PySpark remains available for provenance, review, debugging, CI diff checks, and projects that deliberately
choose generated-code execution.

The normative sources are [Execution](../dev/specifications/Execution.md),
[Execution Semantic Contract](../dev/specifications/ExecutionSemanticContract.md), and
[Online Execution](../dev/specifications/OnlineExecution.md). The execution flow is designed in
[Execution Data Flows](../dev/design/ExecutionDataFlows.md) and
[Online Execution Runtime](../dev/design/OnlineExecutionRuntime.md).

## Plugin-Dispatched Execution

Core owns invocation lifecycle and the standard result boundary, but it no longer owns PySpark runners or target
recipes. A transform resolves exactly one plugin target from `@transform(target=...)`, an explicit `target=`, or
`plugin.default`. Core discovers and negotiates that plugin, creates or retrieves an artifact containing its opaque
payload, and invokes the selected plugin executor.

```text
caller-owned runtime -> StructureSession -> Core artifact workflow -> selected Plugin API executor -> TransformResult
```

`StructureSession` is target-neutral. A PySpark caller supplies a `SparkSession` as its runtime; the PySpark plugin
validates it and performs online execution. Generated output follows the same division: the selected plugin returns
relative source content, while Core validates paths and owns the file write. Plugin-specific runtime values, plans,
schema representations, profile, and variant remain opaque to Core.

Core retains structural ordering, bindings, lanes, hook placement, artifact identity, result wrapping, and diagnostic
presentation. PySpark retains expression semantics, target validation, lowering, schema materialization, online
execution, generated rendering, and Spark Connect behavior. The caller retains Spark lifecycle, reads, writes,
streaming queries, triggers, checkpoints, sinks, and orchestration.

The detailed execution semantics below apply through this boundary. Target selection and target behavior follow the
plugin model above and [Plugin API](../dev/specifications/PluginAPI.md).


## Public API

The default runtime shape is:

```python
from structure import (
    Schema,
    StructureConfig,
    StructureSession,
    StructureTools,
    Transform,
    input,
    lane,
    output,
    raw,
    special,
    step,
    transform,
)
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

Transforms may also be compiled before the first runtime invocation:

```python
EnrichOrders.compile(project_root=".")
```

`compile(...)` builds a reusable in-memory compiled artifact for the transform class and compatible compiler options.
It performs Structure frontend compilation, target capability checks, PySpark recipe lowering, and schema
materialization without binding runtime DataFrames. Later `run(session)` calls reuse the compatible compiled artifact
and still validate each invocation's fresh input DataFrames.

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

If an output declaration has a transform boundary alias, the alias is an additional lookup name, not an extra mapping
key:

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized).alias("orders")

result = NormalizeOrders(orders=orders_df).run(session)

same_df = result.orders
same_schema = result.schema["orders"]
canonical_keys = list(result)  # ["normalized"]
```

The canonical output name remains `normalized`; `orders` is a synonym for result and schema lookup.

Execution evaluates transform methods in source order while preserving independent lane frames. When schemas are
unambiguous, methods consume and update inferred lanes without method-level selectors. Method-level `input=` selects
original inputs or existing lanes, `output=` names intermediate lanes or final outputs, and both options accept ordered
lists. If a lane shares an input name, the lane shadows that original input in method-level `input=`.


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

Generated-code execution may also use explicit in-memory generated artifacts. `MemoryStorage` lets applications call
`Transform.generate(..., storage=MemoryStorage())` and then run generated mode with the same storage object, without
writing generated Python files to disk. This preserves the default "no generated files required" workflow while keeping
generated-code semantics available for no-disk environments.

`target_backend` and `target_profile` remain backend selection inputs. In v1 the only supported backend is `pyspark`.
Future backends should be selected by the session, not by changing transform constructors. Backend support is checked
against the session's resolved `StructureConfig` through [Capabilities](Capabilities.back.md), so
execution and generated-code execution share the same target capability decisions.

Python users may pass a resolved config to the runtime session:

```python
from structure import (
    Schema,
    StructureConfig,
    StructureSession,
    StructureTools,
    Transform,
    input,
    lane,
    output,
    raw,
    special,
    step,
    transform,
)

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

The session compiles a transform on its first compatible run and reuses that result for later invocations.
`Transform.compile(...)` remains available for early diagnostics; load its result with `session.load(artifact)`.
Sessions are isolated by default, while applications may deliberately share a `CompiledArtifactPool`.


## Execution Modes

In direct execution (`execution_mode = "online"`), the session obtains a checked compiled artifact from its pool and
delegates to `OnlinePySparkRunner`.
The runner interprets the artifact's shared PySpark recipes with live PySpark DataFrame and Column APIs. It must not
write generated files and must not execute generated Python source text.

The execution runner must also materialize the transform's Spark schemas from the checked schema model and expose them
on the transform invocation. This gives caller code the same shape contract that generated schema modules provide in
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
2. Resolve every DataFrame explicitly selected by each hook binding.
3. Execute step methods and `@raw` hooks in Transform class declaration order.
4. For each step, lower shared filters and joins, then materialize every ordered result projection.
5. Invoke each raw hook against its selected lane DataFrame at its source-order boundary.
6. Validate intermediate schemas according to project, class, and method policy.
7. Apply hook `schema_mode` and `project_output` rules at the hook boundary.
8. Validate every output DataFrame.
9. Return a read-only `TransformResult`.

Execution and generated-code execution must agree on hook order, validation placement, expression lowering, join
aliasing,
projection shape, schema projection, result shape, and performance guardrails.

For a multi-result step, joins and filters execute once. Each result projection starts from that shared DataFrame and is
stored under its output lane name.

The shared semantic contract is defined in the section below. Execution owns live DataFrame binding and runtime hook
invocation; it must not independently choose aliases, validation placement, expression mapping, or literal typing when a
shared PySpark recipe already defines them.


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

Execution is a runtime boundary and may import PySpark. It requires a local or remote Spark runtime supplied by
the caller.


## Semantic Parity Contract

Online execution and generated-code execution are two consumers of the same checked transform meaning. Online execution
interprets lowered PySpark recipes with live objects; generated execution renders those recipes as deterministic source.
Neither path re-decides projection order, filter order, join aliasing, hook order, validation placement, schema
projection, literal typing, or capability admission.

```text
checked TransformPlan + selected PySpark capabilities
  -> lowered PySpark execution plan
       -> online runner with live PySpark objects
       -> generated PySpark source
```

The shared plan preserves transform identity, capabilities, input bindings, source-ordered steps, final validation,
operation recipes, aliases, schema projections, hook placement, and compiled-path guardrails. The generated emitter does
not invent semantics while rendering text, and the online runner does not execute generated source as an intermediate
step.

Shared invariants include:

- identical source-order operation semantics and output projection;
- identical field aliases, joined-scope aliases, and repeated-join suffixes;
- identical literal typing, expression nullability, and explicit conversion behavior;
- identical hook order, selected inputs, schema mode, and validation boundaries;
- deterministic output for identical source, configuration, target profile, and Structure version;
- identical branch and lane selection, output wrapping, and semantic fingerprints;
- no hidden actions, local row loops, RDD or Pandas fallback, implicit Python UDF fallback, storage writes, or
  streaming lifecycle ownership.

An operation is admitted only when its checked IR and selected backend capabilities can describe its runtime behavior.
Unknown or unsupported required capabilities fail before execution or generation. Source-text formatting, imports, file
headers, and generated module layout remain generator concerns; operation meaning remains this execution contract.


## Streaming Compatibility

Execution does not change the [Streaming](Streaming.back.md) compatibility contract. A transform is
streaming-compatible when
its compiled operations are valid for the caller's streaming DataFrame shape. The caller owns `readStream`,
`writeStream`, triggers, checkpoints, output modes, and query lifecycle.


## Diagnostics

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

See docs/background/Execution.back.md
```


## Plan Cache And Error Boundaries

Compilation may cache immutable plans by source fingerprint, configuration, target, plugin version, and relevant
runtime-support version. Cache hits must not suppress diagnostics or change source anchors. Mutable sessions,
DataFrames, hook instances, and Spark runtime objects remain outside the cached plan.

Runtime errors should preserve Structure diagnostic code and context at declared boundaries. Plugin-specific runtime
errors remain opaque payloads to Core except where the plugin maps them to the shared diagnostic contract.


## Acceptance Contract

Execution is complete when tests prove deferred construction, named input binding, output result access, parity for
every admitted operation family, identical validation boundaries, hook order and ownership, capability rejection before
runtime, streaming classification, generated artifact fingerprint checks, and caller-owned Spark and streaming
lifecycle. Direct and generated execution must expose equivalent output schemas.
