# Streams Example

This example models a white-water kayaking competition with Spark Structured Streaming. Timing messages become
enriched gate passages; the passages support live gate progress and correlation with independently streamed judge
calls. Structure owns the transformations, while callers own stream sources, sinks, checkpoints, triggers, output
modes, and query lifecycle.

## Pipeline map

| Concern | Transform | Result | Streaming contract |
| --- | --- | --- | --- |
| Passage preparation | `PreparePassages` | Enriched `Passage` rows | Watermarked and event-ID deduplicated. |
| Live progress | `BuildGateProgress` | `GateProgress` aggregates | Requires `update` or `complete` output mode. |
| Judge correlation | `CorrelatePenalties` | `Penalty` rows | Two watermarked streams, bounded to five minutes. |

## Prepare passages

`PreparePassages` accepts streaming raw timing events and static `Race`, `Paddler`, and `Gate` reference data. It
rejects negative elapsed times, applies a ten-minute event-time watermark, removes duplicate event IDs, and enriches
each accepted timing message with race, paddler, and gate context. Reference joins are left joins, so a timing event
remains visible when static context is unavailable.

```python
events = spark.readStream.schema(raw_event_schema).json(events_path)
passages = PreparePassages(
    events=events,
    races=races,
    paddlers=paddlers,
    gates=gates,
).run(session).passages

# This relation can feed a sink or another streaming transform.
passage_events = passages.select("race_id", "run_id", "paddler_id", "gate_number", "elapsed_millis")
```

Events older than the watermark may be discarded by Spark. Deduplication relies on the source event ID, so producers
should assign one immutable ID per timing message and preserve it across delivery retries.

## Build live gate progress

`BuildGateProgress` groups watermarked passages by race, run, and gate. For every group it publishes the passage count
and the fastest and slowest elapsed millisecond values observed so far.

```python
progress = BuildGateProgress(passages=passages).run(session).progress
query = (
    progress.writeStream.outputMode("complete")
    .option("checkpointLocation", checkpoint)
    .format("memory")
    .start()
)
```

Because this output is a streaming aggregate, write it in `update` or `complete` mode. The caller chooses how results
are materialized and must use a stable checkpoint location when the query needs recovery.

## Correlate judge penalties

`CorrelatePenalties` joins prepared passages to independently streamed `JudgeCall` rows. Both sides use a ten-minute
watermark. A call matches only when race, run, paddler, and gate agree and the call arrives from the passage time
through five minutes later. The resulting `Penalty` preserves both source identifiers, carries the judge penalty code,
and adds the judge penalty seconds to the passage elapsed time.

```python
calls = spark.readStream.schema(judge_call_schema).json(calls_path)
penalties = CorrelatePenalties(passages=passages, calls=calls).run(session).penalties
penalty_events = penalties.select("event_id", "call_id", "penalty_code", "adjusted_millis")
```

The bounded time condition keeps the stream-stream join state finite. Late, unmatched, or out-of-window calls do not
produce a penalty. A full race winner calculation is intentionally absent: it requires complete-race ranking and
belongs to a future batch telemetry example.
