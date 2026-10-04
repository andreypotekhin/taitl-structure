# V11 Retained V9 Design Gates

## Purpose

V11 carries forward the V9 items that were intentionally kept outside Structure's supported compiler-visible surface.
This design makes each boundary explicit without turning a design decision into a runtime support claim.

Structure owns typed, explainable transformations. Callers own streaming sources, sinks, query lifecycle, retries, and
recovery. A future target profile may relax a boundary only after the corresponding specification, capability entry,
diagnostic, and live evidence exist.

## Status vocabulary

The retained-gates package uses the V11 status vocabulary:

- `design-gated`: a possible Structure contract is identified, but support is not claimed;
- `caller-owned-guided`: the caller may use the upstream API around a Structure transform, with a documented handoff;
- `streaming-ineligible`: a batch shape is admitted or may be admitted, but Structure does not claim its streaming form;
- `unsupported`: the API is deliberately outside the compiler-visible boundary.

`target-gated` describes the required runtime profile in prose; it is not a catalog status.

## Retained items

| Item | V11 status | Required target or condition | Structure boundary | Caller remedy |
| --- | --- | --- | --- | --- |
| Variant mutation helpers | `design-gated` | Released PySpark 4.3+ profile with positive evidence | No mutation lowering until path, null, type, and generated-spelling rules are specified | Use native PySpark Variant expressions in a caller-owned wrapper |
| `is_valid_variant(...)` | `design-gated` | PySpark 4.2 target; positive live evidence is currently unavailable | Capability and negative rejection remain valid; no positive support claim | Use the native PySpark function outside Structure or retain the rejection |
| Chained stateful operators | `design-gated` | Explicit state budget, watermark, retention, output, and restart contract | Only the already admitted two-stage event-time window shape remains supported | Compose separate caller-owned streaming stages |
| Row-level `foreach` | `caller-owned-guided` | Typed handoff for declared final outputs; classic PySpark 3.5 and 4.0 pass isolated live evidence | Structure captures a sink association and returns a writer class plus DataFrame; the caller owns writer instances, query lifecycle, retries, checkpoints, and side effects | Configure and attach the returned handoff with ordinary PySpark APIs |
| Arbitrary state and `transformWithState` | `design-gated` | Typed state schema, timeout, serialization, checkpoint, and recovery contract | No user state store or streaming lifecycle is generated | Use native Structured Streaming state APIs around the transform |
| XML helpers | `unsupported` | No V11 admission | No XML parser or XML source/writer API is exported | Use native Spark XML/provider APIs in caller-owned code |
| Cost-based join reordering | `unsupported` | Separate optimizer design required | Source-authored join order remains authoritative | Arrange joins explicitly in caller code |

## Invariants

Generated Structure modules must contain no `foreach`, `foreachBatch`, `writeStream`, `start`, checkpoint, trigger,
writer construction, state-store, or arbitrary Python lifecycle calls. The compile-time `foreach(row, sink)` marker is
metadata only and is not rendered into generated modules. XML and join-reordering helpers remain unexported. A missing
PySpark 4.2 or 4.3+ runtime is recorded as unavailable evidence and never treated as a passing support result.

## Evidence and diagnostics

Every retained row must have a capability or boundary diagnostic naming the API family, current status, required target
profile when relevant, and caller remedy. Positive support requires online/generated parity and live evidence on the
claimed target. Rejection evidence may run Spark-free; it does not promote the row.
