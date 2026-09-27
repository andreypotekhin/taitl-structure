# Memory Optimization and Query-Plan Troubleshooting

## Purpose

This guide describes how to investigate PySpark driver memory and construction-time failures caused by oversized Spark
query plans. It is a methodology and use-case guide, not a replacement for an issue record. The resolved two-row OOM
case is
the reference example; its exact measurements, design decisions, and acceptance evidence remain in
[I09272602 Spark Driver Heap Exhaustion](../issues/I09272602.Spark-driver-heap-exhaustion.issue.md) and
[Memory-evidence.md](../issues/I09272602/Memory-evidence.md).

## What this guide covers

The relevant resource is the Spark driver while it builds, analyzes, serializes, or explains a query plan. This is
different from executor memory, shuffle spill, or persisted-data capacity.

Use this guide when:

- a tiny fixture spends minutes before collecting a small result;
- a JVM `OutOfMemoryError: Java heap space` appears during query-plan analysis or serialization;
- generated and online execution take similar time before row collection;
- repeated joins, unions, assertions, or fan-out branches reuse an already-expanded DataFrame; or
- a temporary view, alias, cache, or persist appears to help physical reuse but does not shorten construction time.

The guide does not prescribe a universal checkpoint policy. A checkpoint changes failure recovery, storage, and
evaluation behavior, so the application must choose the boundary deliberately.

## Investigation method

### 1. Identify the memory owner and phase

Record the backend, Spark version, driver heap, executor settings, elapsed time, and first meaningful failure. Separate:

- preparation and compilation;
- query-plan construction or explanation;
- checkpoint or other materialization work;
- row collection; and
- cleanup.

A driver heap failure during analysis is not an executor-memory failure. Increasing executor memory or changing shuffle
settings does not address that phase.

### 2. Minimize the reproducer

Reduce the input to a few rows while preserving the operations that reuse an expanded DataFrame. A useful reproducer
should keep the same join, projection, union, assertion, or fan-out shape as the failing graph. If the failure survives
the reduction, input cardinality is not the controlling variable.

The solved OOM reproducer starts with two rows and repeats:

1. alias the current DataFrame twice;
2. self-join the aliases;
3. select a canonical pair;
4. project the reverse pair; and
5. union both directions.

The checked-in [spark_driver_heap_oom.py](../../troubleshooting/memory/spark_driver_heap_oom.py) contains baseline,
fused, checkpoint-every-round, and temporary-view controls.

### 3. Measure query-plan growth

Capture plan-size or construction-time measurements at bounded intervals. Compare the ordinary graph with:

- a semantics-preserving algebraic reduction that removes one repeated branch;
- a true `checkpoint()` or `local_checkpoint()` boundary; and
- a temporary view, alias, `cache()`, or `persist()` control used only as a negative comparison.

Do not treat a shorter unresolved view name or a successful cache request as proof that the query plan was cut.

### 4. Prove the boundary

A real boundary must change the dependency that Spark analyzes. For ordinary PySpark, verify that checkpointed output
has a stable relation shape and that later work no longer contains the prior expanded operations. For Spark Connect,
only admit a capability after a live version-specific test proves rows, schema, cleanup, and plan separation.

The current implementation admits reliable batch `checkpoint(eager=True)` for the explicit Spark Connect 4.0 profile.
It does not generalize that evidence to local checkpointing, streaming checkpoints, or mixed-version Connect profiles.

### 5. Preserve semantics and parity

Every optimization comparison must retain:

- row values and multiplicity;
- schema and nullability expectations;
- strict validation failures;
- sentence-splitting and other intentional UDF behavior;
- generated/online equality; and
- cleanup of Structure-owned temporary artifacts.

A reduction that only makes a disconnected fixture pass is not evidence that the production graph is safe.

## Solved use case: repeated lazy query-plan growth

The former Memory specification's two-row case grew the baseline query plan approximately as
`L_(n+1) = 4.5 L_n + fixed operation text`. With a 1 GiB PySpark 3.5 driver, the baseline reached
`OutOfMemoryError: Java heap space` at round seven. The same operation shape with a typed projection-union fusion
reduced one multiplier to about 2.2x but still grew. Checkpointing every round kept plan text between 46 and 48
characters through eight rounds.

The resolution therefore has three distinct levels:

- **Diminish:** fuse an eligible deterministic projection-union branch to remove one repeated copy.
- **Bound:** use `checkpoint()` or `local_checkpoint()` at an application-approved reuse point.
- **Remove:** redesign the recurrence around a small stable base relation when the business semantics permit it.

The compiler-visible materialization operations and `PYSPARK-W2701` through `PYSPARK-W2704` diagnostics implement
these controls. The diagnostics are advisory: they identify structural risk and remedies, not a universal heap-safety
guarantee.

## Related performance guide

Search construction-time measurements, phase timing, plan-boundary comparisons, and benchmark interpretation are in
[Performance.opt.md](Performance.opt.md). Keep this guide focused on driver memory ownership and proving a true
query-plan boundary.

## Interpretation of common controls

| Control | What it can improve | What it does not prove |
| --- | --- | --- |
| `cache()` / `persist()` | Physical reuse and recomputation cost | The query plan is shorter |
| Python alias or assignment | Readability and references | Spark received a new dependency boundary |
| Temporary view | A named relation and sometimes a visible plan cut | The nested logical plan was discarded |
| Projection-union fusion | One repeated branch and its construction cost | Recursive reuse is bounded |
| `checkpoint()` | A durable query-plan boundary | That the boundary is correct for every application |
| `local_checkpoint()` | A faster, less durable query-plan boundary | Recovery safety or streaming compatibility |
| Larger driver heap | More room for a diagnostic comparison | A production-quality plan shape |

## Diagnostic checklist

For a new report, record:

- exact test or transform and backend;
- Spark/PySpark version and driver heap;
- input size and whether the failure survives a tiny fixture;
- construction, checkpoint, collection, and cleanup timings;
- first driver-side error and whether executor tasks were active;
- plan-growth measurements before and after each control;
- generated/online and schema comparisons; and
- the selected remedy: diminish, bound, or remove.

Link the report to the concrete issue record rather than copying all measurements into a general guide.

## References

- Resolved issue: [I09272602 Spark Driver Heap Exhaustion](../issues/I09272602.Spark-driver-heap-exhaustion.issue.md)
- Detailed evidence: [Memory-evidence.md](../issues/I09272602/Memory-evidence.md)
- End-user guidance: [spark_driver_heap_oom.gotcha.md](../../troubleshooting/memory/spark_driver_heap_oom.gotcha.md)
- Reproducer: [spark_driver_heap_oom.py](../../troubleshooting/memory/spark_driver_heap_oom.py)
- Materialization and diagnostics plan:
  [P08232601](../planning/P08232601.PySpark-lineage-materialization-and-diagnostics.plan.md)
- Performance methodology: [Performance.opt.md](Performance.opt.md)
