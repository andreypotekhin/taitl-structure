# PySpark Integration Performance Troubleshooting

## Purpose

This page is the short troubleshooting entry for slow PySpark integration tests and transforms. It focuses on time spent
constructing, analyzing, serializing, and materializing Spark query plans before rows are collected.

The detailed Search investigation remains in the open
[I09272601 Search Integration Performance](../../dev/issues/I09272601.Search-integration-performance.issue.md) record.
The repeatable measurement method is in [Performance.opt.md](../../dev/optimization/Performance.opt.md).
For an easy-to-follow example and remedies, see the [Search integration slow gotcha](search_integration_slow.gotcha.md).

## Symptoms

A fixture with a few rows can still take minutes when:

- online and generated execution spend most of their time before collection;
- row collection is fast but query-plan construction is slow;
- a graph reuses branches after joins, unions, assertions, or reranking;
- intermediate stage schemas are analyzed even when the test does not use them; or
- Spark Connect spends time sending or serializing a large plan.

This is related to driver-side query-plan growth, but a slow run does not by itself prove a heap failure. For memory
symptoms and boundary troubleshooting, see the [driver-heap gotcha](../memory/spark_driver_heap_oom.gotcha.md) and
[Memory troubleshooting](../memory/Memory.trbl.md).

## First response

Record the backend, Spark/PySpark version, driver settings, test selector, preparation time, online/generated
construction time, checkpoint time, collection time, cleanup time, total time, and exit status. Keep fixtures, runtime
settings, and test inputs identical when comparing a change.

Use the opt-in query-plan profiler to separate plan explanation from checkpoint work:

    STRUCTURE_PROFILE_QUERY_PLANS=1

Run at least three alternating repetitions of the baseline and candidate policy before drawing a conclusion. Report
medians and retain individual runs so a long third run is not hidden by an average.

## Common remedies

- Keep unused intermediate stage outputs disabled in focused Search tests.
- Reuse equivalent policy and validation checks within one transform run.
- Add a batch-only materialization boundary before a large candidate branch is reused when the application contract
  allows it.
- Use `plan_boundaries="auto"` for shared batch subgraphs; compare with `"off"` for diagnosis and `"strict"` only
  for boundary diagnostics.
- Keep streaming frames out of batch-only compiler boundaries.
- Do not treat a larger driver heap, `cache()`, `persist()`, a Python alias, or a temporary view as proof that the
  query plan is smaller or that the production graph is safe.

For Search, keep `SearchDocuments` as the public component and use the existing
`FuseDocuments(materialize=~streaming)` policy. Preserve ranking, schemas, intentional UDF behavior, and
online/generated parity while changing construction strategy.

## Current Search evidence

The Search issue began with 140.63 seconds of combined online/generated construction in the focused Spark 3.5
reranking case, while collection remained below one second per mode. Guard reuse and earlier materialization reduced
the comparable
combined construction to 56.79 seconds in the final profiled run, but broader backend comparisons did not establish a
universal speedup. The issue remains open pending repeated closure benchmarks.

See the [benchmark report](../../dev/issues/I09272601/SearchIntegrationPerformance.bench.md),
[Search proving evidence](../../dev/issues/I09272601/Search-proving-heap.evidence.md), and
[Search lineage follow-up](../../dev/issues/I09272601/Search-lineage-followup.evidence.md).

## References

- Easy example: [search_integration_slow.gotcha.md](search_integration_slow.gotcha.md)
- Open issue:
  [I09272601 Search Integration Performance](../../dev/issues/I09272601.Search-integration-performance.issue.md)
- Benchmark evidence:
  [SearchIntegrationPerformance.bench.md](../../dev/issues/I09272601/SearchIntegrationPerformance.bench.md)
- Driver-memory troubleshooting: [Memory.trbl.md](../memory/Memory.trbl.md)
- Memory optimization methodology: [Memory.opt.md](../../dev/optimization/Memory.opt.md)
