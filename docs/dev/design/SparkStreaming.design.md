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
- hooks only when the author explicitly marks them `streaming=True`;
- `structure explain` and compatibility reports showing `compatible`, `batch_only`, or `unknown`.

The slice admits generated-code execution and execution equally. A feature is not first-slice streaming-supported until both
runtime paths have parity tests or documented manual evidence with a real streaming source.

## Relationship To Compatibility Checks

`docs/dev/specifications/StreamingCompatibility.spec.md` defines the operation-level compatibility model. This first-slice
design promotes the compatible subset from "classification exists" to "support is demonstrable." The compatibility
checker remains conservative: unknown hook bodies and unsupported analytical operations must not become streaming by
accident simply because ordinary batch execution works.

The transform-level marker is an explicit all-step capability contract:

```python
@transform(streaming=True)
class EnrichOrders(Transform):
    ...
```

If the marker is present, every concrete step is checked as streaming-capable. It does not turn batch inputs or outputs
into runtime streaming data. Streaming input declarations and composed streaming outputs trigger the same analysis even
without the marker, while actual output mode follows lineage.

Composition uses `stream_to_batch_policy = "default"` by default. It accepts an undeclared boundary only when the
downstream compiler-visible code is proven compatible; unknown code reports `STREAM-E0802` and incompatible operations
report `STREAM-E0801`. `"strict"` requires explicit `streaming=True` or `allow_stream_to_batch=True`. The allowance
does not suppress a known incompatibility, and explicit `streaming=False` is always rejected.

## Capability Boundary

The PySpark target capability profile owns the support decision. Required first-slice capabilities are:

```text
streaming.row_local_projection
streaming.row_local_filter
streaming.schema_only_validation
streaming.stream_static_left_join
streaming.stream_static_inner_join
streaming.streaming_hook_boundary
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

- "Transform `EnrichOrders` is marked `streaming=True`, but `group_by(...)` requires streaming state.
  Structure's first streaming slice supports row-local projection/filtering, schema-only validation, and stream-static
  left/inner joins only."
- "Joined input `customers` is treated as static for streaming compatibility. Passing a streaming DataFrame for this
  input would create a stream-stream join, which is outside the first slice."
- "Hook `drop_bad_rows` is opaque. Mark it `streaming=True` only if it returns a DataFrame and avoids Spark
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
`SparkStreamingDeferredFeatures.design.md`.

## Caller-Owned Streaming Migration

The next streaming transformation slice keeps the same ownership boundary while admitting compiler-visible state. A
caller may retain `readStream`, `writeStream`, checkpoint, trigger, output-mode, and query lifecycle code and replace
only the typed DataFrame transformation. The initial admitted shapes are static-gap session-window aggregation,
bounded stream-stream outer and semi joins, and stream-static semi filtering.

`session_window(event_time, gap)` is a typed grouping key. The first form requires a positive static Spark interval,
the same event-time field to be watermarked earlier in the step, and at least one ordinary business grouping key.
Explain reports caller-required `append` mode. Dynamic gaps, global session groups, missing or mismatched watermarks,
and invalid gaps fail before execution.

`rowset_join(...)` remains the outer-join spelling and `exists(...)` remains the semi-join spelling. Stream-stream
outer and semi joins require declared streaming inputs, watermarks on both bound event-time fields, and an
`event_time_between(...)` predicate. They require caller-applied `append` mode, and diagnostics explain that unmatched
outer rows can wait for watermark progress. Stream-static `exists(...)` stays a row-preserving left-semi filter with no
watermark or output-mode requirement; the streaming relation must remain on the left.

The IR and shared PySpark recipe carry operation kind, input modes, watermarks, event-time bounds, cardinality, required
output mode, and compatibility status. Online execution and generated rendering consume the same facts. Structure must
not add sources, sinks, checkpoints, triggers, output-mode calls, query lifecycle calls, actions, Pandas/RDD boundaries,
or hidden UDFs. Chained stateful operators, dynamic session gaps, sorting/limits, analytic windows, selected-row
helpers, arbitrary state, and lifecycle ownership remain outside this migration slice until separately designed.

Evidence includes symbolic, capability, compatibility, generated-source, explain, online/generated parity, and live
restart tests on the supported PySpark target lines. Test-owned fixtures own only their temporary source, sink,
checkpoint, and query cleanup.

## Caller-Owned Streaming Adoption Gate

Streaming adoption proceeds in independently verified stages. Stage one admits static stream enrichment with typed
inner, left, and left-semi joins; the current relation remains streaming and on the left, the lookup relation is
explicitly static, and projecting lookup fields requires a unique key or deterministic deduplication policy. Stage two
admits left-outer static lookup after live evidence proves that unmatched streaming rows retain nullable lookup fields.
Stage three permits exactly one already-admitted stateful operation followed only by stateless projection, filtering, or
stream-static enrichment.

Every stage rejects right/full/cross/anti directions, a streaming lookup side, nondeterministic duplicate selection,
additional stateful operators, generators, ordering/limits, analytic windows, and selected-row helpers. Explain shows
input modes and state facts, generated modules remain free of lifecycle calls, and file-stream restart evidence must
pass on PySpark 3.5 and 4.0 before the stage is claimed.

## Streaming Design Gates

The current outstanding gate register is maintained in [Design](../Design.md#design-gates). The sections below retain
the durable streaming boundary and admission rationale.

The v9 design-gate program treats each open family as a proving lane rather than a generic backlog label. Every admitted
stateful feature records its event-time source, watermark, grouping or partition key, state family, caller-required
output mode, allowed following state stage, generated public PySpark form, and corrective diagnostic.

The first candidate chained-state shape is a two-stage event-time window rollup. A typed `window_time(...)` expression
may consume only a `TimeWindow` produced by the existing streaming `window(...)` helper. The accepted form has one
watermarked input, one first-stage tumbling or sliding aggregate, only stateless work between stages, and one second
aggregate over `window_time(first_window)`. Arbitrary nested windows, session chains, a third stateful operation, and a
second stateful family remain rejected. The caller owns `append` mode unless target evidence proves a narrower rule.

Selected-row helpers are split between finite window-scoped forms and global forms. A candidate window-scoped form needs
a watermark, a grouping window, deterministic order keys, and an explicit tie policy; global latest/earliest selection
over an unbounded stream remains a batch boundary. Broad analytic projections such as ranking, lag/lead, and rolling
windows remain batch-only unless a distinct finite-window API proves bounded state and output-mode semantics.

`foreach` and `foreachBatch` are side-effect and lifecycle APIs, not transform operations. `foreachBatch` is
caller-owned-guided through the streaming adoption example; generated transform modules must contain no side-effect
sink calls. Row-level `foreach` remains gated until sink identity, idempotence, retry, security, and recovery contracts
exist. Arbitrary state APIs such as `applyInPandasWithState` and `transformWithState` need a separate typed state model
covering input, state, output, timeout, clock, initialization, cleanup, profile gating, and restart behavior.
