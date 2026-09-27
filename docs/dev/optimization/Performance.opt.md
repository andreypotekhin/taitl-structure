# PySpark Integration Performance Optimization

## Purpose

This guide describes a repeatable way to improve PySpark construction and execution time without hiding semantic
regressions. It treats preparation, query-plan construction, materialization, collection, and cleanup as separate
phases. The Search integration issue is the primary worked example; exact results remain in its issue record and
benchmark report.

## Performance model

A small input does not imply a small Spark workload. A compiler graph can duplicate upstream query plans at fan-out
points, repeat strict validation branches, expose unused stage outputs, or serialize a deep plan through Spark Connect.
The first task is to identify which phase dominates:

| Phase | Typical signal | Useful control |
| --- | --- | --- |
| Preparation | source compilation or fixture setup dominates | reuse shared snapshots and generated setup |
| Construction | build/analysis dominates | reuse guards; reduce branches; add approved batch boundaries |
| Materialization | checkpoint or snapshot dominates | move the boundary to the smallest reusable batch relation |
| Collection | rows or actions dominate | optimize the actual Spark computation and collection pattern |
| Cleanup | generated imports or temporary views linger | time and remove Structure-owned artifacts |

Do not call a construction-time problem an executor slowdown until executor work is shown to dominate.

## Measurement protocol

1. Fix the backend, Spark version, driver settings, test selector, fixture, and environment.
2. Enable the query-plan profiler when phase detail is needed with `STRUCTURE_PROFILE_QUERY_PLANS=1`.
3. Alternate the baseline and candidate policy rather than running all baselines first.
4. Run at least three repetitions per policy and report medians plus individual runtimes.
5. Record preparation, online construction, generated construction, checkpoint/materialization, collection, cleanup,
   total time, warnings, and heap failures.
6. Verify rankings, rows, schema, strict validation errors, intentional UDF behavior, and online/generated equality.
7. Treat a warmed run, a different driver heap, or a different fixture as separate evidence.

The benchmark report must say whether construction timings include checkpoint execution, whether collection was warmed,
and whether Compose startup and cleanup are included in total runner time.

## Compiler-level controls

### Shared batch-plan reuse

Use `plan_boundaries="auto"` to cut only shared batch subgraphs consumed by multiple downstream steps or outputs.
Use `"off"` to reproduce pre-optimization behavior and `"strict"` for diagnostics. The policy is backend-neutral,
but runtime boundaries are batch-only:

    if not frame.isStreaming:
        frame = apply_plan_boundary(frame, spark)

Streaming frames retain their original lineage and never receive compiler temporary-view boundaries.

### Search materialization

Keep `SearchDocuments` as the single public component. `FuseDocuments(materialize=~streaming)` owns the existing
conditional materialization so Search callers do not manage intermediate artifacts. Materialize the smallest reusable
candidate relation before reranking or uniqueness checks, while retaining the final feedback-branch checkpoint.

### Guard and branch reuse

Prefer bounded Spark-side validation that examines no more data than required and reuses equivalent checks within one
transform run. Fuse a safe deterministic projection-union branch when it removes a repeated copy without changing row
multiplicity. Neither optimization is a substitute for a true query-plan boundary when the remaining graph still grows.

### Intermediate validation and stage exposure

Intermediate schema validation is an explicit diagnostic opt-in for Spark Connect, not the default performance path.
Focused tests should disable unused stage-output exposure. These switches must not remove final input/output validation
or strict user-facing safety checks.

## Worked use case: Search integration

The focused Search reranking case originally spent 140.63 seconds constructing online and generated graphs on ordinary
Spark 3.5, while each result collection took less than one second. The compiled graph contained 92 steps, 157 joins,
52 singleton-policy checks, six `require_all` assertions, and two `require_unique` assertions.

Guard reuse and earlier materialization reduced comparable profiled construction to 56.79 seconds and avoided the
driver-heap failure in the final measured run. Other backend comparisons were mixed: `auto` is not a universal speedup
claim, and ordinary Spark 3.5 requires repeated closure benchmarks. The issue remains open until its stated closure
conditions are met.

The measurements and failed alternatives are recorded in
[SearchIntegrationPerformance.bench.md](../issues/I09272601/SearchIntegrationPerformance.bench.md). The implementation
and resolution context are in [I09272601](../issues/I09272601.Search-integration-performance.issue.md).

## Boundaries and memory

A performance improvement can expose or prevent driver memory exhaustion, but these concerns are not interchangeable.
Use [Memory.opt.md](Memory.opt.md) for the general diminish/bound/remove methodology and
[I09272602 Spark Driver Heap Exhaustion](../issues/I09272602.Spark-driver-heap-exhaustion.issue.md) for the resolved
two-row OOM evidence.

`cache()` and `persist()` may reduce recomputation but do not shorten a query plan. A checkpoint is both a performance
control and a semantic/storage decision; measure its execution separately and verify cleanup.

## Reporting template

A useful optimization report includes:

- issue identifier and exact reproduction;
- backend and runtime versions;
- unchanged input, fixture, and driver settings;
- phase timings for every repetition;
- median and individual values;
- plan-boundary, validation, and stage-output policies;
- ranking/schema/UDF/parity results;
- heap failures or warnings; and
- remaining limitations and closure conditions.

## References

- Troubleshooting entry: [Performance.trbl.md](../../troubleshooting/performance/Performance.trbl.md)
- Search issue: [I09272601 Search Integration Performance](../issues/I09272601.Search-integration-performance.issue.md)
- Benchmark report: [SearchIntegrationPerformance.bench.md](../issues/I09272601/SearchIntegrationPerformance.bench.md)
- Search evidence: [Search-proving-heap.evidence.md](../issues/I09272601/Search-proving-heap.evidence.md)
- Shared-plan boundary plan: [P09262601](../planning/P09262601.Shared-plan-boundaries.plan.md)
- Guard reuse plan: [P09262602](../planning/past/P09262602.Search-guard-reuse.plan.md)
- Memory methodology: [Memory.opt.md](Memory.opt.md)
