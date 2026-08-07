# Streaming Reference

Structure can compile a typed transformation that receives a Spark Structured Streaming DataFrame and returns another
DataFrame plan. Use this page when you need to declare streaming inputs, add watermarks, choose a bounded state shape,
check compatibility, or hand the result to a caller-controlled sink.

The [Streaming background](../background/Streaming.back.md) explains the design and state model. The
[Streaming API](../api/Streaming.api.md) lists the streaming operation catalog. The examples use `RawEvent`,
`CleanEvent`, and related names as application-defined `Schema` classes; see the [Schema reference](Schema.ref.md) for
their declarations.

## Decide the application boundary

Streaming support covers the typed DataFrame transformation. The application creates the source and controls the
writer, checkpoint, trigger, output mode, query lifecycle, deployment, and recovery policy.

Start with a streaming DataFrame, pass it to a transform, and configure the returned DataFrame in application code:

```python
from structure import *
from structure.plugin.pyspark import *


events = spark.readStream.schema(raw_event_schema).json(events_path)
clean = CleanEvents(events=events).run(session).clean

query = (
    clean.writeStream
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .format("parquet")
    .start(output_path)
)
```

Structure does not generate or call `readStream`, `writeStream`, `outputMode`, `start()`, `stop()`,
`awaitTermination()`, checkpoint, trigger, or sink APIs inside a transform. A streaming-compatible result is still a
DataFrame plan; it is not a running query.

## Declare streaming inputs

An input declared with `streaming=True` tells the compiler that the relation may carry streaming lineage. An input
without the option is static, which is the usual declaration for reference data used by a lookup join.

```python
@transform(streaming=True)
class CleanEvents(Transform):
    events = input(RawEvent, streaming=True)
    clean = output(CleanEvent)

    def clean_event(self, event: RawEvent) -> CleanEvent:
        where(event.event_id.is_not_null())
        return CleanEvent.project(event)(
            event_id=event.event_id,
            account_id=event.account_id,
            occurred_at=event.occurred_at,
        )
```

The decorator and input option serve different purposes:

| Declaration | Purpose |
| --- | --- |
| `input(Event, streaming=True)` | Declare streaming lineage for one input |
| `input(Event)` | Declare a static input, including a lookup relation |
| `@transform(streaming=True)` | Require every compiled step to satisfy streaming compatibility |
| `@transform(allow_stream_to_batch=True)` | Permit a deliberate undeclared composed boundary |

The transform marker does not turn a batch DataFrame into a stream. The concrete DataFrame supplied at runtime still
determines whether the result is streaming.


## Configure compatibility checks

Compatibility checks classify the operations and input lineages before execution. Enable the checks in project
configuration when the project should report streaming findings for transforms that do not opt in explicitly:

```toml
[tool.structure]
streaming_compatibility_checks = true
stream_to_batch_policy = "default"
allow_stream_to_batch = false
```

The marker changes the severity of findings. The classification is useful even when the application runs a transform
only in batch mode because it exposes a future streaming incompatibility early:

| Situation | Result |
| --- | --- |
| Checks disabled and no marker | No streaming diagnostics |
| Checks enabled and no marker | Known incompatibilities and unknown boundaries are warnings |
| `streaming=True` | Known incompatibilities and unknown boundaries are errors |
| Streaming input or composed streaming result | Streaming analysis is triggered by lineage |

Use `structure check`, `structure compile`, or `structure explain` to inspect compatibility without starting a Spark
query. See the [CLI reference](CLI.ref.md) for command options.

## Preserve streaming lineage through composition

Composition passes the child transform's output lineage to the downstream transform. Use explicit streaming inputs when
the downstream stage is expected to continue processing a stream:

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

`stream_to_batch_policy = "default"` accepts an undeclared downstream boundary only when the downstream operations
are proven compatible. Use `"strict"` when every boundary must be explicit. `allow_stream_to_batch=True` satisfies the
declaration guard for a deliberate boundary, but it cannot suppress a known incompatible operation. Explicit
`streaming=False` remains a compilation error for streaming lineage.


## Use row-local operations

Row-local projections and filters do not retain cross-row state. They are the simplest streaming-compatible shape when
each output row depends only on the current input row:

```python
@transform(streaming=True)
class NormalizeEvents(Transform):
    events = input(RawEvent, streaming=True)
    normalized = output(NormalizedEvent)

    def normalize(self, event: RawEvent) -> NormalizedEvent:
        where(event.event_id.is_not_null())
        return NormalizedEvent.project(event)(
            event_id=lower(trim(event.event_id)),
            account_id=event.account_id,
            occurred_at=to_timestamp(event.occurred_at),
        )
```

Keep expressions compiler-visible. Row-local `Column` expressions, schema-only validation, typed generators, and
ordinary scalar `@special(type="udf")` expressions can be admitted when the selected PySpark target supports them.
Spark actions, local collection, RDD conversion, Pandas conversion, and opaque unmarked hooks are not row-local
transformations.

## Add an event-time watermark

A watermark tells Spark how much event-time lateness the stateful operation may retain. Declare it on the same timestamp
field before the aggregate, deduplication, or join that uses the field:

```python
@transform(streaming=True)
class RecentEvents(Transform):
    events = input(RawEvent, streaming=True)
    recent = output(CleanEvent)

    def retain(self, event: RawEvent) -> CleanEvent:
        watermark(event.occurred_at, delay="10 minutes")
        where(event.event_id.is_not_null())
        return CleanEvent.project(event)
```

`delay` is a non-negative fixed Spark interval such as `"10 minutes"` or `"2 hours"`. The field must be a typed
timestamp expression. A watermark is a transformation declaration; it does not configure a source, trigger,
checkpoint, output mode, or query restart policy.

## Build a windowed aggregate

Use a time window when the result should contain one aggregate row per event-time interval and business-key
combination. A fixed or sliding event-time window requires a preceding watermark on that same event-time field:

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
            slowest_millis=max(passage.elapsed_millis),
        )


progress = GateProgress(passages=passages).run(session).progress
query = progress.writeStream.outputMode("append").start(progress_path)
```

Event-time windows produce a typed `TimeWindow` value with `start` and `end` timestamps. The caller commonly uses
`append` or `update` mode for the aggregate, depending on the sink and target rules. Explain output reports the admitted
modes; Structure does not apply one.


## Build a session-window aggregate

Use a session window when records belong to one activity period separated by a fixed inactivity gap. A session
aggregate needs a watermark on the same event-time field and at least one ordinary grouping key:

```python
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


sessions = PaddlerSessions(passages=passages).run(session).sessions
query = sessions.writeStream.outputMode("append").start(session_path)
```

The gap must be a positive fixed Spark interval. Dynamic gaps, a missing watermark, a mismatched event-time field, or a
session grouped without an ordinary key is rejected before the query starts.

## Chain a bounded event-time window

`window_time(...)` converts a `TimeWindow` back to its event-time timestamp for one admitted second window stage. Use
it only after a watermarked first event-time aggregate and stateless work between the two aggregates:

```python
@transform(streaming=True)
class RollUpWindows(Transform):
    events = input(RawEvent, streaming=True)
    totals = output(WindowTotal)

    def roll_up(self, event: RawEvent) -> WindowTotal:
        watermark(event.occurred_at, delay="1 hour")
        first_window = window(event.occurred_at, "5 minutes")
        group_by(first_window, account_id=event.account_id)
        where(event.event_id.is_not_null())
        second_event_time = window_time(first_window)
        group_by(window(second_event_time, "1 hour"), account_id=event.account_id)
        return WindowTotal.project(event)(total=sum(event.amount))
```

The admitted chained shape is one first window aggregate, stateless work, and one second event-time aggregate. A third
stateful stage, nested window construction, a second join or deduplication stage, and arbitrary state processing remain
unsupported.

## Deduplicate within a watermark

Use watermark-bounded deduplication when a stable event identifier should be accepted once while late duplicates remain
possible. The explicit helper makes the streaming requirement visible:

```python
@transform(streaming=True)
class UniqueEvents(Transform):
    events = input(RawEvent, streaming=True)
    unique = output(CleanEvent)

    def deduplicate(self, event: RawEvent) -> CleanEvent:
        watermark(event.occurred_at, delay="10 minutes")
        drop_duplicates_within_watermark(event.event_id)
        return CleanEvent.project(event)
```

`drop_duplicates(...)` is cross-mode: Structure uses the batch form for batch inputs and a watermark-bounded streaming
form for streaming inputs. `drop_duplicates_within_watermark(...)` requires a declared streaming input and a prior
watermark. Neither helper means that the source will retain records forever.

## Join a stream to static reference data

Use a stream-static join when the current relation is streaming and the lookup relation is static. Declare the lookup
input without `streaming=True`, then project nullable lookup fields deliberately:

```python
@transform(streaming=True)
class EnrichEvents(Transform):
    events = input(RawEvent, streaming=True)
    accounts = input(Account)
    enriched = output(EnrichedEvent)

    def enrich(self, event: RawEvent, account: Account) -> EnrichedEvent:
        left_join(
            account,
            on=account.id == event.account_id,
            hint="broadcast",
        )
        return EnrichedEvent.project(event)(
            account_name=account.name,
            account_tier=account.tier,
        )
```

Left and inner stream-static joins are admitted when the static side and join shape satisfy the target policy. A
broadcast hint applies to the static side only. A static declaration does not prove key uniqueness; use the join
diagnostics and the [Join reference](Join.ref.md) when duplicate lookup rows would change the result grain.

## Join two bounded streams

Use a stream-stream join only when both inputs are declared streaming, both event-time fields are watermarked, and the
predicate bounds the time relationship:

```python
@transform(streaming=True)
class CorrelateEvents(Transform):
    events = input(RawEvent, streaming=True)
    acknowledgements = input(Acknowledgement, streaming=True)
    correlated = output(CorrelatedEvent)

    def correlate(
        self, event: RawEvent, acknowledgement: Acknowledgement
    ) -> CorrelatedEvent:
        watermark(event.occurred_at, delay="10 minutes")
        watermark(acknowledgement.occurred_at, delay="10 minutes")
        inner_join(
            acknowledgement,
            on=(acknowledgement.event_id == event.event_id)
            & event_time_between(
                event.occurred_at,
                acknowledgement.occurred_at,
                upper="5 minutes",
            ),
        )
        return CorrelatedEvent.project(event)(
            acknowledgement_id=acknowledgement.acknowledgement_id,
        )
```

The watermarks and event-time bound make retention visible to the compiler. Supported bounded outer and semi forms use
the same declarations and require the corresponding join direction. Missing input modes, missing watermarks, an
unbounded predicate, or an unsupported direction produces a streaming diagnostic before query start.


## Filter by stream existence

Use `exists(...)` when the right relation decides whether a current streaming row is eligible but no right-side fields
belong in the result:

```python
@transform(streaming=True)
class AdmitEvents(Transform):
    events = input(RawEvent, streaming=True)
    allowed_accounts = input(AllowedAccount)
    admitted = output(CleanEvent)

    def admit(self, event: RawEvent, allowed: AllowedAccount) -> CleanEvent:
        where(exists(on=event.account_id == allowed.id))
        return CleanEvent.project(event)
```

For a stream-static existence filter, keep the streaming relation on the left and the side input static. A bounded
stream-stream semi form also requires both input declarations, watermarks, and an event-time bound.

## Inspect required output modes

`StreamingOutputMode` is Structure's typed vocabulary for modes reported by compatibility and explain output. It does
not replace the PySpark writer call. Apply the selected mode to the returned DataFrame in application code:

```python
progress = GateProgress(passages=passages).run(session).progress

# The mode is selected from the transform's reported requirements and sink policy.
query = progress.writeStream.outputMode("update").option(
    "checkpointLocation", checkpoint
).start(progress_path)
```

Use these as orientation, then confirm the exact report and target constraints:

| Shape | Common modes | Important condition |
| --- | --- | --- |
| Stateless projection or filter | `append` | No stateful aggregate or join |
| Event-time aggregate | `append`, `update` | Matching watermark on grouped event time |
| Session aggregate | `append` | Fixed positive gap and matching watermark |
| Business-key aggregate | `update`, `complete` | State may be unbounded; `STREAM-W0802` may be emitted |
| Bounded stream-stream join | `append` | Both watermarks and event-time bound |

## Mark a raw hook as streaming-safe

Use `@raw(..., streaming=True)` only when the hook body is an application-controlled PySpark DataFrame transformation
that returns a DataFrame and avoids actions, local collection, RDD/Pandas conversion, lifecycle calls, external side
effects, and unmodeled state:

```python
from pyspark.sql import functions as F


@transform(streaming=True)
class WithValidNames(Transform):
    events = input(RawEvent, streaming=True)
    valid = output(CleanEvent)

    @raw(inout=events | valid, streaming=True)
    def keep_valid(self, *, events, spark, ctx):
        return events.where(F.col("event_id").isNotNull())
```

Structure records the declaration but does not prove arbitrary hook code safe. An unmarked hook makes compatibility
unknown, and `streaming=True` turns that unknown boundary into a compile error when the transform opts into streaming.

## Run and compile a streaming transform

Use the ordinary execution API for a live DataFrame plan. `run(...)` does not start a query, so it is safe to compose
the result with application-controlled writer code:

```python
events = spark.readStream.schema(raw_event_schema).json(events_path)
result = CleanEvents(events=events).run(session)
query = result.clean.writeStream.outputMode("append").start(output_path)
```

Use `compile(...)` when compatibility and target diagnostics should be checked without binding live DataFrames:

```python
artifact = CleanEvents.compile(project_root=".")
```

Online execution and generated-code execution must agree on input lineage, operation order, watermark placement,
compatibility status, output schemas, and reported output modes. Generated modules remain transformation-only modules.


## Attach a caller-controlled `foreachBatch` sink

Use `foreachBatch` when the application must write each micro-batch through a batch-oriented sink. Keep the callback,
checkpoint, retry behavior, and query start outside Structure:

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

Before starting a side-effecting query, validate a stable sink identity, idempotence key, retry policy, and snapshot
identity in application code. The callback must honor those declarations across retries. Row-level `foreach` remains
design-gated.

## Review an arbitrary-state boundary

`applyInPandasWithState` and `transformWithState` are not Structure state runtimes. Record the typed state boundary,
timeout policy, checkpoint identity, and restart policy before writing native caller-controlled state code:

```python
state_review = {
    "input_schema": Event,
    "key_schema": EventKey,
    "state_schema": EventState,
    "output_schema": EventResult,
    "grouping_key": ("account_id",),
    "timeout_policy": "event_time",
    "timeout_duration": "1 hour",
    "checkpoint_identity": "events-state-v1",
    "state_version": "event-state-v1",
    "restart_policy": "same_checkpoint",
}
```

Review that the state boundary has typed schemas, grouping keys, timeout clock and duration, target profile,
initialization/update/removal behavior, checkpoint identity, state version, and restart behavior. This record does not
generate a state processor, start a query, or prove recovery.

## Know the streaming-ineligible surface

Structure rejects a streaming shape when its state, cardinality, or lifecycle cannot be established from
compiler-visible declarations. Common ineligible operations include:

| Shape | Correction |
| --- | --- |
| Global ordering, limit, offset, or top-N | Keep the transform batch-only or use a bounded event-time aggregate |
| Ranking, lag/lead, rolling, or global selected-row helpers | Use a grouped event-time aggregate or batch mode |
| Stream-stream join without watermarks and time bound | Add both watermarks and `event_time_between(...)` |
| Dynamic session gap | Use a positive fixed interval |
| Stateful operation without matching watermark | Declare the watermark before the operation |
| RDD, Pandas, action, or local collection | Keep it outside the transform or use a separate batch path |
| Unmarked raw hook | Keep it batch-only or prove and mark the hook `streaming=True` |
| Source, sink, checkpoint, trigger, or query lifecycle in a transform | Move it to application code |

This rejection is deliberate. Spark may support a broader shape under a specific query or state policy, but Structure
admits only forms whose requirements can be reported and kept equivalent in online and generated execution.


## Correct streaming diagnostics

Use the diagnostic code and the named operation to choose the smallest correction:

| Diagnostic | Meaning | Correction |
| --- | --- | --- |
| `STREAM-E0801` | Operation is incompatible with streaming lineage | Replace it or add its required bound |
| `STREAM-E0802` | Result crosses an undeclared streaming boundary | Declare `streaming=True` or allow it explicitly |
| `STREAM-W0802` | Aggregate state may be unbounded | Use event-time eviction or accept the policy |

For example, a bounded stream-stream join needs all three pieces named in its correction:

```text
CompileError STREAM-E0801: Transform is not streaming-compatible

Problem:
  stream-stream joins require declared streaming inputs, watermarks on both event-time fields,
  and an event-time bound.

Use:
  declare both inputs streaming=True, add matching watermark(...), and add event_time_between(...),
  or keep the transform batch-only.
```

See [Diagnostics.md](../Diagnostics.md) for the complete diagnostic catalog. For state and lifecycle rationale, see
[Streaming background](../background/Streaming.back.md).

## See also

- [Streaming background](../background/Streaming.back.md)
- [Streaming API](../api/Streaming.api.md)
- [Transform reference](Transform.ref.md)
- [Aggregations reference](Aggregations.ref.md)
- [Join reference](Join.ref.md)
- [Execution reference](Execution.ref.md)
- [Configuration reference](ConfigSchema.ref.md)
