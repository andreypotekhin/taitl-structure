# Execution Reference

Execution is the default way to run a transform. The caller supplies a Spark session and input DataFrames;
`StructureSession` checks the transform, invokes the selected target runtime, and returns named DataFrame results.

The [Execution background](../background/Execution.back.md) defines lifecycle and parity rules. The
[Getting Started guide](../GettingStarted.md) provides a complete first transform.

Examples use the schemas and transform shapes introduced in the [Schema reference](Schema.ref.md) and
[Transform reference](Transform.ref.md). Replace them with your application's declarations.

## Run a transform

Call `run(...)` when the application is ready to execute a declared transform against its input DataFrames.

```python
from structure import *
from structure.plugin.pyspark import *

session = StructureSession(spark=spark, ctx=ctx)
result = EnrichOrders(
    orders=orders_df,
    customers=customers_df,
).run(session)

enriched = result.enriched
enriched_schema = result.schema.enriched
```

Construction is deferred:

```python
invocation = EnrichOrders(orders=orders_df, customers=customers_df)
# No Spark action occurs above.
result = invocation.run(session)
```

Transform inputs are keyword-only and must use declared names. Unknown or missing inputs are errors. Structure does not
write, publish, or cache returned DataFrames; the caller performs those operations.

The session can also run an invocation directly:

```python
result = session.run(EnrichOrders(orders=orders_df, customers=customers_df))
```

## Results

`run(...)` returns a read-only `TransformResult` for one or many outputs. Use declared names rather than positional
tuples:

```python
result = Fulfillment(orders=orders_df, warehouses=warehouses_df).run(session)
plans = result.plans
shortages = result["shortages"]

assert result.schema.plans == plans.schema
```

`result.schema.<name>` and `result.schema["<name>"]` expose materialized PySpark `StructType` values. An output alias
is an additional lookup name, not a second mapping key; the canonical declared name remains stable. There is no
automatic `df` alias unless an output is explicitly named `df`.

### Result access and control

| Result surface | Contract |
| --- | --- |
| `result.output_name` | Canonical DataFrame lookup |
| `result["output_name"]` | Equivalent named lookup |
| `result.schema.output_name` | Materialized output `StructType` |
| `result.schema["output_name"]` | Equivalent schema lookup |
| `list(result)` | Canonical output names, in declaration order |

The result is read-only. Structure does not write, cache, publish, collect, or convert a returned DataFrame. A caller
may persist a result, start a streaming sink, or pass it to another transform after `run(...)` returns.

An output boundary alias is a synonym:

```python
class NormalizeOrders(Transform):
    orders = input(OrderRaw)
    normalized = output(OrderNormalized).alias("orders")

result = NormalizeOrders(orders=orders_df).run(session)
assert result.normalized is result.orders
assert list(result) == ["normalized"]
```

The alias does not add a second result key or rename schema fields.

## Compile before running

Call `compile(...)` first when source and target diagnostics should be separated from live DataFrame execution.

```python
artifact = EnrichOrders.compile(project_root=".")
result = EnrichOrders(orders=orders_df, customers=customers_df).run(session)
```

`compile(...)` discovers source, captures symbolic operations, checks types and target capabilities, lowers the selected
plugin plan, and materializes target schema information without binding live DataFrames. The session reuses compatible
immutable artifacts while validating each invocation's fresh inputs.

Compilation is keyed by source identity, configuration, selected target, plugin version, and relevant runtime-support
version. A cache hit may reuse an immutable plan, but it never reuses live DataFrames, hook instances, or Spark runtime
objects. It must not suppress diagnostics or change source anchors.

## Execution modes

| Mode | Behavior | Generated files required? |
| --- | --- | --- |
| `online` | Interpret the checked PySpark plan with live DataFrame and Column objects | No |
| `generated` | Import the generated PySpark class and execute its `run(...)` method | Usually yes |

Select a mode in configuration or a resolved Python config:

```python
from structure import *

config = StructureConfig.resolve(project_root=".", execution_mode="generated")
session = StructureSession(spark=spark, config=config)
```

Generated mode can use an in-memory generated artifact when a no-disk workflow calls for generated semantics. If the
generated class is unavailable, Structure reports how to run `structure compile`, make the generated root importable,
or switch to `online`.

Generated execution imports the generated class, constructs it with the session's Spark and context, and calls its
`run(...)` method with the invocation's stored named inputs. Online execution interprets shared PySpark recipes with
live DataFrame and Column objects; it does not execute generated Python source as an intermediate step.

`MemoryStorage` can hold generated artifacts for no-disk workflows:

```python
from structure import *

storage = MemoryStorage()
config = StructureConfig.resolve(execution_mode="generated")
EnrichOrders.generate(storage=storage)
session = StructureSession(spark=spark, config=config, storage=storage)
result = EnrichOrders(orders=orders_df, customers=customers_df).run(session)
```

The storage choice changes artifact packaging, not transform meaning. An artifact remains tied to its source,
configuration, target profile, and semantic fingerprint.

## Session responsibilities

`StructureSession` contains the resolved configuration, selected target, materialized schemas, and a
session-local compiled-artifact pool. It accepts:

```python
session = StructureSession(
    spark=spark,
    ctx=ctx,
    project_root=".",
    execution_mode="online",
    generated_package="structure_generated",
)
```

The caller controls Spark startup and shutdown, DataFrame reads and writes, streaming sources and sinks, triggers,
checkpoints, output modes, and orchestration. A session does not silently change Spark configuration or start a query.

Repeated compatible runs reuse the checked plan but never reuse live DataFrames or suppress input diagnostics. Sessions
are isolated by default; applications may deliberately share a compiled-artifact pool.

`StructureSession.close()` releases Structure-owned remote plan boundaries or temporary views where the selected target
uses them. Closing the session does not stop or reconfigure the caller's Spark session. Close a generated transform's
Structure-owned resources after lazy results no longer need them when the runtime exposes that lifecycle.

## Runtime order

Both online and generated execution follow this order:

1. validate declared inputs;
2. resolve DataFrames selected by hook bindings;
3. execute steps and hooks in source order;
4. lower filters and joins and materialize ordered projections;
5. validate intermediate schemas according to policy;
6. apply hook schema mode and output projection;
7. validate final outputs;
8. return the read-only result.

The two modes share operation meaning, field aliases, joined scopes, literal typing, nullability, projection shape,
hook order, validation boundaries, and semantic fingerprints. Generated source is a rendering of the checked plan; the
online runner does not execute generated Python as an intermediate step.

For multi-result steps, filters and joins execute once on the shared input shape, then each ordered projection is stored
under its output lane name. Hook boundaries remain in source order in both modes. A generated source diff is therefore a
meaningful review artifact, not an alternate implementation with independent semantics.

```python
invocation = PublishOrders(orders=orders_df)
# Construction does not validate rows or start a Spark action.
result = invocation.run(session)
# Input, steps, hooks, intermediate lanes, and final outputs are checked in the declared order.
published = result.published
```

A failure at one boundary stops the run with that boundary's diagnostic; later outputs are not silently returned as if
the transform had completed.

## Validation

Input, intermediate, and output validation settings are controlled by `StructureConfig`:

```toml
[tool.structure]
validate_inputs = true
validate_intermediate = true
input_validation_mode = "schema_only"
intermediate_validation_mode = "schema_only"
output_validation_mode = "schema_only"
```

`schema_only` validates shape without row scans. Constraint checks use a separate opt-in mode and can add Spark work.
Hook `SchemaMode.STRICT` is the normal boundary; `ALLOW_EXTRA_COLUMNS` permits additional hook columns where the
transform contract allows them.

Input validation checks declared names, physical aliases, data types, nested structure, and reliable nullability.
Intermediate validation checks step and lane contracts. Output validation checks every published DataFrame. A hook may
use `project_output=True` with an extra-column schema policy to restore the declared output shape at its boundary.

## Streaming

Execution preserves the streaming compatibility classification of the checked transform. It does not own the query
lifecycle:

```python
streaming_df = spark.readStream.schema(event_schema).json(path)
result = WindowedOrders(events=streaming_df).run(session)

query = result.totals.writeStream.outputMode("append").start(output_path)
```

Callers choose `readStream`, `writeStream`, triggers, checkpoints, sinks, and output modes. Structure only compiles and
executes the admitted transformation shape. See the [Streaming API](../api/Streaming.api.md) for watermarks,
event-time windows, dedupe, and bounded join conditions.

`streaming=True` is a compatibility declaration, not a query-start switch. Explicit `input(..., streaming=False)` or
an explicitly non-streaming composed boundary remains an error when it receives streaming lineage. A deliberate
stream-to-batch boundary requires the documented configuration allowance and a caller-controlled materialization step.

## Errors and remedies

| Error situation | Remedy |
| --- | --- |
| Missing or unknown input | Bind the declared DataFrame by its keyword name |
| Input schema mismatch | Inspect the validation diagnostic and `result.schema` |
| Generated class unavailable | Run `structure compile` or use `execution_mode = "online"` |
| Hook failure | Inspect selected lane, target, and schema mode |
| Unsupported operation | Check the relevant API page for target and streaming conditions |
| Stale artifact | Regenerate or clear the configured artifact storage |

Runtime errors retain the Structure diagnostic code, transform, lane/output, execution mode, target, and shortest
source-level correction. A compile or validation failure never becomes an empty result.

Common error boundaries are:

```text
missing declared input       -> bind the input by its declared keyword name
generated class unavailable   -> run structure compile or switch to online mode
schema mismatch               -> inspect result.schema and the validation diagnostic
hook failure                  -> inspect the selected lane, target, and schema mode
stale artifact                -> regenerate or clear the configured artifact storage
```

Plugin-specific runtime failures remain target-owned except where the plugin maps them to the shared diagnostic
contract. Structure retains the transform, result or lane, execution mode, selected target, and source-level remedy.

## Before running

- Supply an existing Spark session; Structure never creates or stops one.
- Construct transforms with declared keyword input names.
- Compile early when diagnostics should be separated from runtime execution.
- Choose `online` when generated files are not part of the workflow.
- Choose `generated` when source artifacts, review, or generated-code execution are required.
- Read outputs and schemas through their canonical names.
- Validate inputs, intermediate lanes, and outputs at the intended cost.
- Keep hooks and streaming lifecycle application-controlled.
- Compare online and generated results when changing a target or generation option.

```python
result = EnrichOrders(orders=orders_df, customers=customers_df).run(session)
for name in result:
    print(name, result.schema[name])
```

Iterate canonical output names and inspect schemas through the read-only result wrapper instead of assuming a positional
tuple or a default `df` attribute.

## Input binding and lane flow

`Transform.__init__(**inputs)` stores DataFrames using declared Structure input names. Positional arguments, unknown
names, and custom runtime parameters are rejected. Runtime context belongs in `StructureSession(ctx=...)`; it should
not be smuggled into a transform constructor.

Methods execute in declaration order. When schemas are unambiguous, the compiler infers lane flow. Use method-level
`input=` to select an original input or existing lane, and `output=` to select an intermediate lane or final output.
Both accept ordered lists. A lane with the same name as an original input shadows that input for later inferred
bindings, so use role selectors when the distinction matters.

```text
declared inputs
  -> input validation
  -> step projection/filter/join
  -> intermediate lane
  -> hook boundary, if declared
  -> later step or named output
  -> final validation
```

The output wrapper preserves declared names and output order. A transform with several outputs does not return an
untyped tuple and does not invent a `df` key.

## Parity verification

When a workflow uses both modes, compare more than rows:

```python
online = EnrichOrders(orders=orders_df, customers=customers_df).run(online_session)
generated = EnrichOrders(orders=orders_df, customers=customers_df).run(generated_session)

assert online.schema.published == generated.schema.published
assert online.published.schema == generated.published.schema
assert collect_rows(online.published) == collect_rows(generated.published)
```

The parity contract includes field aliases, nested types, nullability where the target exposes it reliably, row values,
extra-column behavior, operation order, hook order, and expected diagnostics. A generated renderer must not re-decide
semantics while producing source text.

## Application-controlled lifecycle

Execution does not control:

- Spark session creation or shutdown;
- source DataFrame reads and output writes;
- `readStream` or `writeStream`;
- checkpoints, triggers, output modes, and query restart;
- orchestration, scheduling, transactions, or external side effects.

This remains true in generated mode. Generated code is a callable artifact, not a deployment manager. A caller may use
the returned DataFrames in a larger application, but that application controls the lifecycle decisions.

```python
stream = spark.readStream.schema(event_schema).json(input_path)
result = WindowedOrders(events=stream).run(session)
query = result.totals.writeStream.option("checkpointLocation", checkpoint).start(output_path)
```

The caller created the source and starts the query; `run(session)` only constructs the admitted transformation result.

## Execution contract at a glance

| Boundary | Structure guarantees | Caller guarantees |
| --- | --- | --- |
| Invocation | Named input binding and deferred construction | DataFrames are available when run begins |
| Compile | Typed plan, target checks, and diagnostics | Source modules are import-safe |
| Runtime | Shared operation order and result wrapping | Spark runtime and context are valid |
| Validation | Declared schema checks at configured phases | Chosen validation cost is acceptable |
| Streaming | Compatibility classification | Caller controls stream lifecycle and restart |
| Generated | Deterministic artifact semantics | Generated source is compiled/importable when selected |

The contract is intentionally asymmetric: Structure describes and executes transformations, while the application
decides when and where data is read, written, materialized, retried, or published.

## Resource hygiene

Do not retain a session artifact pool or generated remote plan longer than the application needs it. Use
one session for compatible invocations that share configuration and target; create an isolated session when lifecycle,
context, or target selection should be isolated. Close Structure-owned resources after lazy results are materialized or
released, but do not use `close()` as a substitute for stopping application-controlled Spark or streaming resources.

```python
session = StructureSession(spark=spark, config=config)
try:
    result = EnrichOrders(orders=orders_df, customers=customers_df).run(session)
    result.enriched.write.mode("overwrite").parquet(output_path)
finally:
    session.close()
    # The caller still decides when to stop `spark`.
```

## Runtime troubleshooting order

When a run fails, inspect boundaries in this order:

1. Confirm the invocation uses every declared input name exactly once.
2. Compare incoming DataFrame shape with the declared input Schema.
3. Read the selected execution mode and target from the session configuration.
4. Inspect the named step, lane, hook, and validation phase in the diagnostic.
5. Compare `result.schema` or the expected generated schema with the failing boundary.
6. If generated mode is selected, verify the artifact fingerprint and import path.
7. If streaming is involved, verify application-controlled watermark, output-mode, checkpoint, and restart assumptions.

```python
try:
    result = SearchDocuments(requests=requests, scores=scores).run(session)
except Exception as error:
    diagnostic = error.diagnostic
    print(diagnostic.code, diagnostic.use_text())
```

Start with the named Structure boundary before inspecting Spark state or changing execution mode.

Do not repair a runtime schema mismatch by disabling all validation. Narrow the phase or correct the source contract so
the failure remains visible at the boundary where it can be fixed.

For an intermittent failure, first compare the source/configuration/target fingerprint and artifact reuse decision.
Only then inspect live Spark state or input data. The session cache is an optimization boundary, not a source of
semantic variation.

The safest recovery is to correct the named boundary and rerun the same
source/configuration/target combination. Changing execution mode should be a deliberate comparison, not an
error-suppression technique.

## Action timing and recovery

Transform construction and compilation are planning operations. They should not trigger a Spark action, write an
output, mutate an application DataFrame, or start a streaming query. Runtime evaluation begins only when `run(session)`
is called and the selected execution mode accepts the invocation.

If a runtime action fails, preserve the original exception and its Structure boundary in the diagnostic. A retry is safe
only when the caller's source, sink, checkpoint, and side-effect policy make it safe. Structure does not infer
idempotence, replay a partially written sink, or restart a streaming query on the caller's behalf.

For generated execution, validate the artifact before retrying a live run. For online execution, compare the resolved
target and configuration with the failing run. A mode switch can isolate an artifact or dispatch problem, but it cannot
prove that the underlying transformation is correct.

```python
invocation = EnrichOrders(orders=orders_df, customers=customers_df)
# Planning only; no action has started.
plan = EnrichOrders.compile(project_root=".")
result = invocation.run(session)
```

Compile or construct first when you need a Spark-free diagnostic; call `run(...)` only at the runtime boundary.

## See also

- [Transform reference](Transform.ref.md)
- [Configuration reference](ConfigSchema.ref.md)
- [Generated PySpark Source](../GeneratedSource.md)
- [Execution background](../background/Execution.back.md)
