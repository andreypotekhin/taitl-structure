# Spark Streaming First Slice Design

Spark Structured Streaming support starts as caller-owned streaming execution, not generated streaming job ownership.
Structure should let a developer pass a streaming DataFrame into the same execution or generated-code execution shape used for
batch work when every compiler-visible operation is valid for that streaming shape. The first slice turns the existing
streaming compatibility classification into a tested support claim for a narrow, useful surface.

## Design Position

The first slice keeps Spark streaming inside the PySpark target. It does not add a new backend, a streaming runtime, or
a job orchestration DSL. The caller still creates sources, static lookup DataFrames, sinks, triggers, checkpoints, and
query lifecycle code outside Structure.

The generated and online APIs stay unchanged:

```python
orders = spark.readStream.table("orders")
customers = spark.read.table("customers")

session = StructureSession(spark=spark, ctx=ctx, config=config)
online = EnrichOrders(orders=orders, customers=customers).run(session)

generated = EnrichOrdersGenerated(spark=spark, ctx=ctx).run(
    orders=orders,
    customers=customers,
)
```

Both calls return a DataFrame. If `orders` is streaming and the transform is compatible, the result is streaming. The
caller decides how to start the query:

```python
query = generated.writeStream.option("checkpointLocation", checkpoint).toTable("orders_enriched")
```

Structure must not call `readStream`, `writeStream`, `start()`, `awaitTermination()`, trigger APIs, checkpoint APIs, or
storage write APIs in this slice.

## First Slice Surface

The supported runtime shape is one streaming current pipeline input plus zero or more static side inputs. The current
pipeline input is the DataFrame that flows through source-ordered step methods. Static side inputs are named
inputs used for lookup joins.

The first slice supports:

- row-local projection;
- row-local filtering;
- schema-only validation;
- left and inner stream-static lookup joins;
- static-side broadcast hints when the existing join hint model supports them;
- compiler-visible expressions that lower to Spark Column operations without actions, UDFs, local collection, or RDD
  conversion;
- hooks only when the author explicitly marks them `streaming_safe=True`;
- `structure explain` and compatibility reports showing `compatible`, `batch_only`, or `unknown`.

The slice admits generated-code execution and execution equally. A feature is not first-slice streaming-supported until both
runtime paths have parity tests or documented manual evidence with a real streaming source.

## Relationship To Compatibility Checks

`docs/dev/specifications/StreamingCompatibility.md` defines the operation-level compatibility model. This first-slice
design promotes the compatible subset from "classification exists" to "support is demonstrable." The compatibility
checker remains conservative: unknown hook bodies and unsupported analytical operations must not become streaming by
accident simply because ordinary batch execution works.

The transform-level marker stays the user-facing commitment:

```python
@transform(streaming_compatible=True)
class EnrichOrders(Transform):
    ...
```

If the marker is present, unsupported or unknown operations fail as errors. If the marker is absent and streaming
checks are enabled, incompatible operations can remain warnings so batch-only projects are not broken by the presence
of the checker.

## Capability Boundary

The PySpark target capability profile owns the support decision. Required first-slice capabilities are:

```text
streaming.row_local_projection
streaming.row_local_filter
streaming.schema_only_validation
streaming.stream_static_left_join
streaming.stream_static_inner_join
streaming.streaming_safe_hook_boundary
```

Deferred streaming capabilities remain explicitly unsupported:

```text
streaming.source_generation
streaming.sink_generation
streaming.trigger_policy
streaming.checkpoint_policy
streaming.output_mode
```

Unsupported means these caller-owned lifecycle concerns must fail early instead of silently becoming generated
orchestration. Stateful transformation support requires explicit compiler-visible state semantics.

## Diagnostics

Streaming diagnostics should state the runtime-shape assumption. Good messages name the transform, operation, and
reason, then point to the public streaming reference.

Examples:

- "Transform `EnrichOrders` is marked `streaming_compatible=True`, but `group_by(...)` requires streaming state.
  Structure's first streaming slice supports row-local projection/filtering, schema-only validation, and stream-static
  left/inner joins only."
- "Joined input `customers` is treated as static for streaming compatibility. Passing a streaming DataFrame for this
  input would create a stream-stream join, which is outside the first slice."
- "Hook `drop_bad_rows` is opaque. Mark it `streaming_safe=True` only if it returns a DataFrame and avoids Spark
  actions, RDD/Pandas conversion, streaming lifecycle APIs, and stateful operations."

Diagnostics should link to `docs/background/Execution.back.md` for the support boundary and to
`docs/background/Execution.back.md` for intentionally deferred features.

## Testing And Evidence

Sprint 09 support needs evidence beyond static classification:

- compiler tests proving incompatible operations become warnings or errors with the right severity;
- direct runtime tests with a streaming source such as Spark's rate source or a memory stream equivalent;
- generated-code runtime tests for the same fixture;
- generated-source scans proving no lifecycle calls or actions are emitted;
- explain tests showing streaming compatibility status and the reason for batch-only or unknown operations.

If CI cannot reliably run streaming integration tests, Sprint 09 should add a documented manual verification script and
make its result release-blocking for the support claim.

## Design Consequences

The first slice is intentionally small because streaming failures are often operational rather than syntactic. By
leaving lifecycle ownership with the caller, Structure can provide value immediately: typed transform authoring,
execution/generated-code parity, schema checks, explain output, and clear compile-time diagnostics without taking over query
deployment.

Full streaming orchestration should build on this slice later by adding explicit source, sink, trigger, checkpoint,
output mode, watermark, and state-policy models. Those are designed separately in
`SparkStreamingDeferredFeatures.md`.
