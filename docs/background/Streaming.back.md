# Streaming

Structure supports compiler-visible Spark Structured Streaming transformations while leaving streaming lifecycle with
the application. The application creates streaming and static DataFrames, passes them to an ordinary Structure
transform, and controls sources, sinks, checkpoints, triggers, output modes, query start/stop, deployment, and recovery.

The [Streaming reference](../reference/Streaming.ref.md) lists practical declarations and corrections. The
[Streaming API](../api/Streaming.api.md) lists supported declarations and parity. This background combines runtime
shape, compatibility analysis, supported operations, deferred features, hooks, validation, and generated-code rules.
The normative sources are [Spark Streaming](../dev/specifications/SparkStreaming.spec.md),
[Streaming Compatibility](../dev/specifications/StreamingCompatibility.spec.md), and the
[Spark Streaming design](../dev/design/SparkStreaming.design.md).

## Runtime Ownership Boundary

The supported shape is one streaming current pipeline input plus optional static side inputs:

```python
orders = spark.readStream.table("orders")
customers = spark.read.table("customers")

result = EnrichOrdersGenerated(spark=spark).run(
    orders=orders,
    customers=customers,
)

query = result.writeStream.option("checkpointLocation", checkpoint).toTable("orders_enriched")
```

Structure maintains the checked transformation plan and returns a DataFrame plan. It does not generate `readStream` or
`writeStream`, start or stop queries, set checkpoints or triggers, apply output modes, or perform external side effects.

Streaming compatibility means the generated or online transformation can accept the concrete streaming DataFrame shape,
and every operation on streaming data is admitted by the compiler-visible policy. It does not mean Structure starts a
streaming query.

The same transform class can describe batch and streaming use. The input declaration records the expected lineage for
compatibility analysis; the DataFrame supplied by the application determines the runtime shape:

```python
class PrepareEvents(Transform):
    events = input(RawEvent, streaming=True)
    clean = output(CleanEvent)

    def clean_event(self, event: RawEvent) -> CleanEvent:
        where(event.event_id.is_not_null())
        return CleanEvent.project(event)(
            event_id=event.event_id,
            occurred_at=event.occurred_at,
        )


batch_result = PrepareEvents(events=spark.read.table("events")).run(session)
stream_result = PrepareEvents(events=spark.readStream.table("events")).run(session)
```

The declaration permits streaming analysis. It does not create a source or force a batch DataFrame to become
streaming. A declared static input is appropriate for lookup data and prevents a lookup operation from silently
becoming a stream-stream join.


## Declaring Compatibility

Transform-level opt-in is an explicit all-step contract:

```python
@transform(streaming=True)
class EnrichOrders(Transform):
    enriched = output(OrderEnriched)
    ...
```

The configuration seed is:

```toml
streaming_compatibility_checks = true
```

With checks enabled, a non-streaming transform receives warnings for incompatible or unknown shapes, while
`streaming=True` turns incompatible and unknown shapes into errors. If checks are disabled, an explicit transform marker
still runs compatibility analysis for that transform. Streaming inputs and composed streaming outputs trigger analysis
without implicitly changing transform options.

The result classification is:

```text
compatible  every operation is admitted
batch_only  at least one operation is known to be incompatible
unknown     an opaque operation prevents proof, with no known incompatibility
```

An undeclared stream-to-batch boundary follows `stream_to_batch_policy`. A strict policy requires an explicit streaming
declaration or `allow_stream_to_batch=True`; no allowance suppresses a known incompatible operation.

### Compatibility Policy At A Glance

Use this policy matrix to predict whether a streaming finding is silent, advisory, or an error.

```text
checks disabled, no marker       -> no streaming diagnostics
checks enabled, batch transform  -> incompatible shapes warn
checks enabled, streaming=True   -> incompatible or unknown shapes error
marker with checks disabled      -> marker still runs its compatibility pass
```

The marker is an author promise about every operation in the transform. It does not turn batch-only operations into
streaming operations and does not change the application-controlled runtime lifecycle.

### Composition and stream-to-batch boundaries

Composition carries effective streaming lineage from a child result into the downstream transform. The default policy
allows an undeclared downstream boundary only when the downstream compiler-visible operations are proven compatible:

```python
@transform(streaming=True)
class NormalizeEvents(Transform):
    events = input(RawEvent, streaming=True)
    normalized = output(NormalizedEvent)

    def normalize(self, event: RawEvent) -> NormalizedEvent:
        return NormalizedEvent.project(event)(
            event_id=event.event_id,
            occurred_at=event.occurred_at,
        )


@transform(streaming=True)
class PublishEvents(Transform):
    events = input(NormalizedEvent, streaming=True)
    published = output(PublishedEvent)

    def publish(self, event: NormalizedEvent) -> PublishedEvent:
        return PublishedEvent.project(event)


@transform(streaming=True)
class EventPipeline(Transform):
    events = input(RawEvent, streaming=True)
    published = output(PublishedEvent)

    normalized = NormalizeEvents(events=events)
    published = PublishEvents(events=normalized.normalized)
```

Use `stream_to_batch_policy = "strict"` when every composed boundary must be declared. A downstream input can then
declare `streaming=True`, or a transform can explicitly set `allow_stream_to_batch=True` for a deliberate boundary.
That allowance only addresses the declaration guard. It cannot make a known incompatible operation compatible, and
`streaming=False` remains an explicit rejection for streaming lineage.


## Supported Stateless Operations

Projection is compatible when every value is a compiler-visible row-local Spark `Column` expression. Filtering is
compatible when every predicate is a compiler-visible row-local boolean expression. Expression helpers and scalar
`@special(type="udf")` calls are compatible when they do not require cross-row state, local collection, or RDD
conversion.

Schema-only validation is compatible. It may inspect column names, data types, nullability metadata, and nested shape,
but may not trigger Spark jobs. Compiler traceability and generated metadata are compatible when they record
compile-time information only.

Watermarks are compatible when declared with `watermark(field, delay=...)` before the stateful operation they support.
The same transform semantics must apply in online and generated-code execution.

### Stateless Streaming Transform

Projection and filtering remain ordinary compiler-visible operations when their expressions are row-local:

```python
@transform(streaming=True)
class CleanEvents(Transform):
    events = input(RawEvent, streaming=True)
    clean = output(CleanEvent)

    def clean_event(self, event: RawEvent) -> CleanEvent:
        where(event.event_id.is_not_null())
        return CleanEvent(
            event_id=event.event_id,
            account_id=event.account_id,
            occurred_at=to_timestamp(event.occurred_at),
        )
```

Schema-only validation, compiler traceability, typed expressions, and row-local filters do not require a Spark action.
An expression that collects rows, calls Pandas or RDD APIs, or depends on an opaque unmarked hook changes the
classification.


## Stateful Operations And Joins

Batch grouped aggregations remain supported. Streaming business-key aggregations may require the application to apply
`update` or `complete` output modes and can retain unbounded state; Structure reports this advisory risk as
`STREAM-W0802`.
Event-time and session-window aggregations require a compiler-visible watermark on the grouped event-time field.
Watermarked dedupe is admitted only in the documented bounded shapes.

Stream-static joins are allowed when the current pipeline is streaming and the side input is static. `exists(...)` may
provide stream-static left-semi filtering. Broadcast hints apply only to the static side.

Bounded stream-stream joins require both inputs to declare `streaming=True`, watermarks on both sides, and an event-time
bound such as `event_time_between(left_time, right_time, upper=...)`. Structure admits only shapes whose state and
retention policy are compiler-visible.

### Watermarked Stream-Stream Join

Use a bounded stream-stream join when two event-time relations must be correlated while Spark can retain only a finite
matching horizon.

```python
@transform(streaming=True)
class AttributeClicks(Transform):
    impressions = input(Impression, streaming=True)
    clicks = input(Click, streaming=True)
    attributed = output(AttributedClick)

    def attribute(
        self, impression: Impression, click: Click
    ) -> AttributedClick:
        watermark(impression.shown_at, delay="7 days")
        watermark(click.occurred_at, delay="7 days")
        inner_join(
            click,
            on=(click.impression_id == impression.impression_id)
            & event_time_between(
                impression.shown_at,
                click.occurred_at,
                upper="24 hours",
            ),
        )
        return AttributedClick(
            impression_id=impression.impression_id,
            click_id=click.click_id,
            occurred_at=click.occurred_at,
        )
```

The two watermarks bound state retention and the event-time predicate bounds matching. The caller still chooses output
mode, checkpoint, trigger, and sink. A stream-stream join without these declarations is not silently treated as safe.

### Watermarked Dedupe

Use watermark-bounded deduplication when a stable event identifier should be accepted once within the retained event
time horizon.

```python
@transform(streaming=True)
class LatestClicks(Transform):
    clicks = input(Click, streaming=True)
    latest = output(Click)

    def keep_latest(self, click: Click) -> Click:
        watermark(click.occurred_at, delay="7 days")
        drop_duplicates_within_watermark(click.click_id)
        return Click.project(click)
```

The watermark-bounded spelling is explicit. Global `distinct()` or unbounded `drop_duplicates(...)` does not acquire
streaming semantics merely because a watermark appears elsewhere in the transform.

### Event-time and session windows

Use an event-time window when records should be grouped into fixed or sliding time ranges. Use a session window when
activity is grouped by a fixed inactivity gap. Both forms require a matching watermark before the stateful operation:

```python
@transform(streaming=True)
class GateProgress(Transform):
    passages = input(Passage, streaming=True)
    progress = output(GateProgressRow)

    def summarize(self, passage: Passage) -> GateProgressRow:
        watermark(passage.occurred_at, delay="10 minutes")
        group_by(
            window(passage.occurred_at, "1 minute"),
            race_id=passage.race_id,
            gate_number=passage.gate_number,
        )
        return GateProgressRow.project(passage)(
            passage_count=count(),
            fastest_millis=min(passage.elapsed_millis),
        )


@transform(streaming=True)
class PaddlerSessions(Transform):
    passages = input(Passage, streaming=True)
    sessions = output(PaddlerSession)

    def summarize(self, passage: Passage) -> PaddlerSession:
        watermark(passage.occurred_at, delay="30 minutes")
        group_by(
            session_window(passage.occurred_at, "5 minutes"),
            paddler_id=passage.paddler_id,
        )
        return PaddlerSession.project(passage)(passage_count=count())
```

Fixed event-time windows use caller-applied `append` or `update` mode. Session windows use `append` mode and require
at least one ordinary grouping key in addition to the session key. Dynamic gaps, missing watermarks, mismatched event
time fields, and arbitrary chained state remain rejected.

### Output-mode metadata

Structure records the output modes that the caller must apply; it does not call `outputMode(...)`. The practical
mapping is:

| Transform shape | Typical caller mode | State note |
| --- | --- | --- |
| Row-local projection or filter | `append` | No Structure-managed state |
| Watermarked event-time aggregate | `append` or `update` | Watermark bounds event-time state |
| Session-window aggregate | `append` | Fixed gap and watermark are required |
| Business-key aggregate without event-time eviction | `update` or `complete` | State may be unbounded; warning |
| Bounded stream-stream join | `append` | Both watermarks and an event-time bound are required |

The exact mode also depends on the sink and concrete Spark target. Treat explain output as the compatibility report,
then choose and apply the mode in application code.


## API Ledger And Stateful Boundaries

The streaming API ledger classifies each compiler-visible operation as compatible, batch-only, rejected, or unknown for
the concrete input lineage. The classification records operation family, current versus side-input lineage, required
watermark or event-time bound, state retention assumptions, hook declaration, and diagnostic severity. An opaque hook or
unclassified operation cannot silently become compatible.

Chained event-time windows require each stateful stage to declare its event-time input, watermark relationship, and
retention boundary. A later window cannot claim the watermark of an unrelated or already-invalidated field. Selected-row
and analytic-window helpers such as `latest_by(...)`, ranking, lag/lead, rolling windows, and unbounded dedupe remain
batch-only unless a dedicated bounded-state contract admits the exact shape.

Finite grouped `first_value(...)` and `last_value(...)` may be used inside a watermarked event-time aggregate window;
that is not a general streaming reinterpretation of selected-row or analytic-window helpers. Streaming state remains a
target policy, not a hidden Structure-owned store.

The implementation must keep streaming-specific state, output-mode, and retention decisions in capability and
compatibility metadata. It must not create sources, sinks, triggers, checkpoints, query lifecycle, or recovery logic.

## Hooks And Validation

Hooks are opaque because Structure cannot safely inspect arbitrary PySpark code. Mark a hook `streaming=True` only when
it returns a DataFrame, avoids actions, RDD/Pandas conversion, lifecycle APIs, external side effects, and unmodeled
state:

```python
@raw(inout=lane(orders) | lane(orders), streaming=True)
def keep_valid(self, *, orders, spark, ctx):
    return orders.where(F.col("id").isNotNull())
```

Structure trusts the marker and records it in traceability; it does not prove the hook body safe.

Input, intermediate, and output validation is streaming-compatible only when it is schema-only. Row-level constraints,
uniqueness checks, sampling, counts, collections, and other scans classify the plan as batch-only unless a future policy
proves them safe.

Hooks need the same explicit promise:

```python
@raw(inout=lane(events) | lane(events), streaming=True)
def retain_valid(self, *, events, spark, ctx):
    return events.where(F.col("event_id").isNotNull())
```

Structure records the declaration but does not inspect the hook body. The hook must avoid actions, RDD/Pandas
conversion, query lifecycle APIs, external side effects, and unmodeled state.

## Caller-controlled lifecycle

Create sources, invoke the Structure transform, configure the writer, and start the query in application code. A
Structure transform returns a DataFrame plan and may be used as the input to another transform:

```python
events = spark.readStream.schema(raw_event_schema).json(events_path)
clean = PrepareEvents(events=events).run(session).clean

query = (
    clean.writeStream
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .format("parquet")
    .start(output_path)
)
```

Keep `readStream`, `writeStream`, `outputMode`, checkpoints, triggers, `start()`, `stop()`, `awaitTermination()`,
and sink-specific side effects outside transform methods and generated transform modules. A stable checkpoint and a
caller-selected trigger are operational decisions; Structure reports the transformation requirements but does not
choose them.

For a batch sink invoked once per micro-batch, keep `foreachBatch` in that same application layer. Validate the
idempotence and retry policy before starting the query:

```python
def write_batch(batch, batch_id):
    batch.write.format("parquet").mode("append").save(output_path)


query = (
    clean.writeStream
    .foreachBatch(write_batch)
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .start()
)
```

The callback remains responsible for honoring a stable sink identity, idempotence key, retry policy, and snapshot
identity. These declarations are application safeguards; they do not turn `foreachBatch` into a Structure transform
operation.


## Compile-Time And IR Contract

The checker runs after symbolic execution and before code generation, without starting Spark. Each IR operation exposes
its support classification and enough metadata to explain the decision:

- operation kind and source transform/step;
- source location or expression text when available;
- referenced input scopes and current versus side-input lineage;
- join type, hint, watermark, and event-time bound;
- validation mode and hook streaming declaration.

The checker must reject or warn on unsupported operations, incompatible stream-stream joins, unsafe hooks, non-schema
validation, and unknown opaque boundaries. It preserves the classification in compile reports and traceability.


## Generated-Code Requirements

Generated PySpark must use DataFrame and Column operations for compiled DSL operations, avoid actions and RDD/Pandas
conversion, keep schema-only validation action-free, and call streaming-safe hooks without lifecycle wrapping. It may
use
the same generated class for batch and streaming DataFrames because the caller supplies the runtime shape.


## Deferred And Rejected Operations

The following remain batch-only or deferred for streaming inputs:

- global sort, limit, offset, global top-N, and unbounded distinct/deduplication;
- event-time aggregations without a matching preceding watermark;
- chained windowed or stateful aggregations beyond the admitted single-stage shapes;
- ranking, lag/lead, rolling windows, and selected-row helpers such as `latest_by(...)`;
- cross, anti, or unbounded stream-stream joins;
- Pandas UDFs, RDD operations, `mapInPandas`, `foreachPartition`, and local Spark actions;
- arbitrary hooks without an explicit streaming-safe declaration;
- generated lifecycle, custom sinks, `foreachBatch`, `foreach`, and arbitrary state APIs.

Finite grouped `first_value(...)` and `last_value(...)` remain possible inside a watermarked event-time window. They are
aggregate expressions, not a streaming reinterpretation of batch selected-row or analytic-window helpers.

This is rejected even though it is valid batch Structure code:

```python
@transform(streaming=True)
class TopStreamingEvents(Transform):
    events = input(RawEvent, streaming=True)
    top = output(RawEvent)

    def top_events(self, event: RawEvent) -> RawEvent:
        order_by(event.occurred_at)
        limit(100)
        return RawEvent.project(event)
```

Global ordering and limit require an unbounded view of the stream. Keep the transform batch-only, or replace the shape
with an admitted bounded-state policy.


## Diagnostics

Streaming diagnostics should identify the transform, step, concrete input lineage, operation, state assumption, and
shortest fix. Typical fixes are to remove a batch-only operation, make a side input static, add a watermark and
event-time
bound, explicitly mark a safe hook, or keep lifecycle code in application-controlled Spark code.

```text
CompileError STREAM-E0801: Transform is not streaming-compatible

Problem:
  stream-stream joins require declared streaming inputs, watermarks on both event-time fields, and an event-time bound.

Use:
  declare both streaming inputs, add watermarks and event_time_between(...), or keep the transform batch-only.

See docs/background/Streaming.back.md
```


## Appendix: Deferred Feature Admission

A deferred streaming feature may be admitted only when Structure defines its public DSL or configuration, capability
requirements, diagnostics, explain output, online/generated parity, live Spark verification, and troubleshooting
guidance.
Future additions may include state-stage lists, more bounded stream-stream joins, and explicit retention rules, but
sources, sinks, triggers, checkpoints, output modes, deployment, recovery, and external side effects remain
application-controlled.
