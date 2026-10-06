# Memory Troubleshooting

This page is the short troubleshooting entry for driver memory problems caused by oversized Spark query plans. Start
with the common checks, then use the scenario that matches the failing workload.

## Main references

- [Performance runbook](../../runbooks/Performance.runbook.md)
- [Memory runbook](../../runbooks/Memory.runbook.md)
- [Profiling runbook](../../runbooks/Profiling.runbook.md)

## Symptoms

A tiny input can spend most of its time while Spark constructs or analyzes a query plan, then fail with
`java.lang.OutOfMemoryError: Java heap space` in the driver. This is different from an executor or shuffle-memory
failure.

This page owns the memory-specific case: a delay before rows are available combined with driver-side plan analysis,
serialization, checkpointing, or heap exhaustion. A small input does not make an expanded logical plan small.

## First response

Record the backend, Spark/PySpark version, driver heap, input size, time spent constructing the plan, time spent
materializing or collecting rows, and the first driver-side error. Check whether the same failure survives a tiny
fixture. A failure that occurs before executor work begins points to driver-side plan construction.

## Common remedies

When a DataFrame is reused after joins, unions, or other plan-expanding operations, use an explicit
`checkpoint()` or `local_checkpoint()` boundary when the application semantics allow it. `cache()`, `persist()`,
Python aliases, and temporary views may improve physical reuse or naming, but they do not shorten the query plan.
Structure also emits `PYSPARK-W2701` through `PYSPARK-W2704` when it detects costly repeated reuse or fan-out.

Use the diminish, bound, or remove method in the [Memory runbook](../../runbooks/Memory.runbook.md) when choosing a
remedy.

Do not confuse compiled-artifact reuse with a Spark plan boundary. `STRUCTURE_COMPILED_ARTIFACT_REUSE=module` retains
compiler metadata for compatible integration requests; `off` is a diagnostic control for repeated compiler setup. Neither
mode retains Spark frames or results, and neither mode truncates lazy Spark lineage. If the first error is a driver
`OutOfMemoryError` during analysis or checkpointing, follow the plan-growth and materialization guidance below rather than
using compiler reuse as the memory fix.

## Use cases

### Reused lazy lineage

If an iterative transformation repeatedly aliases, joins, projects, or unions a DataFrame that still contains the full
history of earlier rounds, follow the
[Spark driver heap exhaustion gotcha](spark_driver_heap_oom.gotcha.md). It includes the small reproducer, boundary
choices, and practical restructuring guidance.

#### References

- [Spark driver heap exhaustion issue](../../dev/issues/I09272602.Spark-driver-heap-exhaustion.issue.md)
- [Memory evidence](../../dev/issues/I09272602/Memory-evidence.md)
- [Lineage materialization plan][memory-plan]
- [Spark driver heap exhaustion gotcha](spark_driver_heap_oom.gotcha.md)

### The driver fails while building the query

Reduce the graph to a small fixture while preserving its joins, unions, projections, assertions, and reuse shape. If the
failure survives the reduction, input cardinality is not the controlling variable. Record whether executor tasks were
active when the first failure occurred; a driver `OutOfMemoryError` during analysis or checkpointing is not an executor
memory problem.

Use the [Memory runbook](../../runbooks/Memory.runbook.md) to compare one approved checkpoint or materialization
boundary at a time. Do not treat a cache request, alias, temporary view, or larger executor heap as proof that the
driver-side logical plan was shortened.

#### References

- [Memory runbook](../../runbooks/Memory.runbook.md)
- [Spark driver heap exhaustion gotcha](spark_driver_heap_oom.gotcha.md)

### Search or ranking candidate reuse

Retrieval and reranking pipelines can create the same driver-side problem when an expanded candidate relation is reused
by multiple scoring, uniqueness, or feedback branches. Use the
[Search integration performance issue](../../dev/issues/I09272601.Search-integration-performance.issue.md) for
measurements and the [Performance troubleshooting](../performance/Performance.trbl.md) page for phase-specific guidance.

#### References

- [Search integration performance issue](../../dev/issues/I09272601.Search-integration-performance.issue.md)
- [Performance troubleshooting](../performance/Performance.trbl.md)

[memory-plan]: ../../dev/planning/past/P08232601.PySpark-lineage-materialization-and-diagnostics.plan.md
