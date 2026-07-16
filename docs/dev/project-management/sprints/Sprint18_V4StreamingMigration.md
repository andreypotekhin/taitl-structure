# Sprint 18: V4 Caller-Owned Streaming Migration

## Sprint Goal

Make the next common batch-to-stream and plain-PySpark-to-Structure transformations compiler-visible without making Structure a streaming-job owner.

## Product Outcome

Developers can use Structure transforms for static-gap session aggregates, bounded stream-stream outer and semi joins, and stream-static semi filtering, while retaining their existing source, sink, checkpoint, trigger, and query code.

## Scope

### In Scope

- `session_window(event_time, gap)` with static positive gaps and typed `TimeWindow` output.
- Watermarked session aggregation with an ordinary business grouping key and caller-required `append` mode.
- Bounded stream-stream left/right/full outer joins and left-semi joins through existing `rowset_join(...)` and `exists(...)` forms.
- Stream-static semi filtering through `exists(...)`.
- Capability checks, diagnostics, explain output, generated-code and online parity, public migration examples, and live evidence on PySpark 3.5.x and 4.0.x.

### Out of Scope

- Sources, sinks, triggers, checkpoints, output-mode application, query lifecycle, `foreach`, and `foreachBatch`.
- Dynamic session gaps, session merge tuning, chained stateful operations, chained windows, and unbounded state.
- Stream-static right/full/cross/anti shapes, arbitrary state processors, Pandas/RDD boundaries, and Spark Connect streaming.

## ExecPlan

`docs/dev/planning/P07152602.V4-caller-owned-streaming-migration.plan.md`

## Engineering Tasks

1. Add the session-window expression and its schema/type, IR, capability, renderer, and online-recipe support.
2. Extend streaming compatibility and join lowering for the admitted outer and semi shapes.
3. Add diagnostics and explain rendering for watermarks, bounds, stateful cardinality, and required output modes.
4. Add migration fixtures and live PySpark 3.5.x/4.0.x parity evidence without adding lifecycle code.
5. Publish API, reference, troubleshooting, catalog, and gaps updates.

## Acceptance Criteria

- A compatible transform returns a streaming DataFrame plan in online and generated modes for every admitted family.
- Invalid stateful shapes fail before runtime with a corrective diagnostic.
- Explain output reports the operation's state assumptions and `append` requirement where applicable.
- Generated transform bodies contain no lifecycle or action calls.
- Live evidence passes on both supported PySpark lines, and `make build` passes.

## Risks

- Spark outer-output timing depends on watermark progress; examples and diagnostics must make this visible.
- The two target lines must prove the same public contract; a target-specific discrepancy blocks implementation rather than weakening the documentation.

## Progress

- [ ] Start after the v4 relational and advanced analytical coverage slice.

