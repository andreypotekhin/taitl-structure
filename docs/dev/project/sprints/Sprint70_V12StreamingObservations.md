# Sprint 70: V12 Streaming Observations and Progress

Status: planned. Iteration 10 of 12; two weeks, with calendar dates unset.

## Sprint goal

Extend typed batch observations with an ordinary streaming metric delivery model.

## User-facing outcome

Users can inspect typed per-batch quality metrics from caller-owned streaming progress without adding actions.

## Dependencies

Requires Sprints 61, 64, and 69; retain V11 batch observation retrieval and variant-specific behavior.

## Implementation tasks

- [ ] Specify streaming observation declarations, metric schemas, naming, batch/query identity, and progress retrieval.
- [ ] Define duplicate-name, empty-batch, replay/restart, and composed-output behavior before implementation.
- [ ] Implement compiler and generated lowering using native named streaming observations and a typed progress adapter.
- [ ] Keep listener/query attachment, subscription lifetime, query start/stop, and external metric delivery
      caller-owned.
- [ ] Ensure metric evaluation preserves the observed relation and adds no collection, action, or hidden streaming
      query.
- [ ] Add an adoption example using existing state/progress guidance with explicit metric availability.

Follow the [V12 delivery requirements](../V12.md#delivery-and-dependencies) for package design/specification,
user-story tests, diagnostics, generated artifacts, public documentation, and evidence collected within this sprint.

## Specification inputs

- [V11 observations](../../specifications/V11PySpark41Parity.spec.md).
- [Streaming](../../specifications/SparkStreaming.spec.md).
- [Execution](../../specifications/Execution.spec.md).

These are existing inputs. Specify V12 additions and their public API before implementation; a planned task does not
change the current support claim.

## Acceptance

- Typed metric values match native observations per micro-batch without changing rows or schema.
- Restart, empty-batch, replay, and composed-transform cases follow the documented identity and availability rules.
- Batch .get() behavior remains unchanged and Connect streaming receives an explicit unsupported diagnostic.

## Demo and validation

Attach a caller-owned progress callback to a stream with typed count and null-quality metrics.
Process two batches, restart the query, and compare native/online/generated metrics and rows.

```bash
make build
make integration BACKEND=pyspark42
make integration BACKEND=spark-connect42
```

The 4.2 runner selectors become available in Sprint 61. Connect commands exercise batch cases only; ordinary
streaming cases run on the ordinary lane. Run retained-profile cases wherever this sprint claims them.

## Risks

Streaming progress is asynchronous and retries may repeat observations.
Use batch identity and documented availability rather than promising exactly-once external metric delivery.

## Non-goals

No monitoring service, owned listeners/query lifecycle, blocking batch-style .get() on streams, or Connect streaming.

## Governing documents

[V12 charter](../V12.md), [sprint planning guide](../SprintPlanningGuide.md), and
[definition of done](../DefinitionOfDone.md).
