# Streams Example

`examples.streams` models white-water kayaking timing and judging. It deliberately demonstrates only transforms that
Spark Structured Streaming can execute with compiler-visible state rules.

`PreparePassages` normalizes timing messages, enriches them from static race, paddler, and gate reference data, applies
an event-time watermark, and removes duplicate event identifiers. `BuildGateProgress` produces a watermarked aggregate;
the caller must write that result in `update` or `complete` mode. `CorrelatePenalties` joins timing passages with
independently streamed judge calls, bounded to calls reported within five minutes of a matching passage.

Structure owns none of the query lifecycle. The caller creates sources and sinks, owns checkpoint locations, chooses
triggers and output mode, and starts and stops queries. A caller-owned file-stream seam looks like this:

```python
events = spark.readStream.schema(raw_event_schema).json(events_path)
passages = PreparePassages(events=events, races=races, paddlers=paddlers, gates=gates).run(session).passages
progress = BuildGateProgress(passages=passages).run(session).progress
query = progress.writeStream.outputMode("complete").option("checkpointLocation", checkpoint).format("memory").start()
```

`RaceWinner` is intentionally not produced here. Computing winners requires complete-race ranking and belongs to the
future batch-only telemetry example.
