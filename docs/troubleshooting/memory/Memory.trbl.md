# PySpark Driver Memory Troubleshooting

## Purpose

This page is the short troubleshooting entry for driver memory problems caused by oversized Spark query plans. It
explains
the symptom and points to the resolved issue record, detailed evidence, and the end-user gotcha.

## Symptom

A tiny input can spend most of its time while Spark constructs or analyzes a query plan, then fail with
`java.lang.OutOfMemoryError: Java heap space` in the driver. This is different from an executor or shuffle-memory
failure.

## Remedy

When a DataFrame is reused after joins, unions, or other plan-expanding operations, use an explicit
`checkpoint()` or `local_checkpoint()` boundary when the application semantics allow it. `cache()`, `persist()`,
Python aliases, and temporary views may improve physical reuse or naming, but they do not shorten the query plan.
Structure also emits `PYSPARK-W2701` through `PYSPARK-W2704` when it detects costly repeated reuse or fan-out.

For user-facing symptoms, reproduction commands, and practical restructuring guidance, see the
[Spark driver heap exhaustion gotcha](spark_driver_heap_oom.gotcha.md).

## Issue record and evidence

The resolved root-cause and implementation record is
[I09272602 Spark Driver Heap Exhaustion](../../dev/issues/I09272602.Spark-driver-heap-exhaustion.issue.md).
Its moved results, measurements, design decisions, and acceptance evidence are in
[Memory-evidence.md](../../dev/issues/I09272602/Memory-evidence.md).

The implementation contract is tracked in the [lineage materialization and diagnostics plan][memory-plan].
The general developer methodology is in [Memory optimization](../../dev/optimization/Memory.opt.md).
Search construction-time and benchmark guidance is in
[Performance troubleshooting](../performance/Performance.trbl.md) and
[Performance optimization](../../dev/optimization/Performance.opt.md).

[memory-plan]: ../../dev/planning/P08232601.PySpark-lineage-materialization-and-diagnostics.plan.md
