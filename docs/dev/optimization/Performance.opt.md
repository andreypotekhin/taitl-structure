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
8. When preparation or construction may repeat compiler work, compare `STRUCTURE_COMPILED_ARTIFACT_REUSE=off` with
   `module` in fresh runner processes and enable `STRUCTURE_PROFILE_COMPILATION=1`. Keep the generated package,
   plugin, schema registry, validation settings, stage-output policy, and code options identical; a mismatch creates a
   legitimate cache miss rather than evidence that reuse is broken.

The benchmark report must say whether construction timings include checkpoint execution, whether collection was warmed,
and whether Compose startup and cleanup are included in total runner time.

Compilation profiling is a diagnostic for Structure preparation, not a Spark-plan profiler. A `misses=1` record means
that the complete cache request built an artifact; a `hits=1` record means that the compatible artifact was found after
key construction. The request duration includes key construction and cache lookup. Do not add it again when it is nested
inside preparation or construction, and do not interpret a compilation hit as proof that Spark analysis or checkpoint
work was avoided.

## Profiling workflow

Use the [Performance runbook](../../runbooks/Performance.runbook.md) to classify the slow phase and
choose a controlled comparison. Then use the [PySpark performance profiling runbook](../../runbooks/Profiling.runbook.md)
for the copy-paste Compose command, phase log fields, and timing record. The profiling workflow preserves the test's
normal actions, captures the runner exit status, and maps each report field to the corresponding log output.

### Phase timing

The integration timing helper emits completed durations as `[phase] label: Ns`. Record the completed duration, not the
preceding `starting` line. Preparation, online/generated construction, collection, and cleanup labels are test-specific;
record every matching label and state which labels were included in each total.

Checkpoint timing is nested inside construction. The query-plan profiler reports checkpoint explanation and checkpoint
work separately; do not add either value to construction or total a second time. The runbook explains the nesting and
the distinction between pytest time and full Compose wall-clock time.

### Query-plan profiler

Set `STRUCTURE_PROFILE_QUERY_PLANS=1` for structural diagnosis. It reports guard timings, estimated expanded input
references, checkpoint explanation time, checkpoint duration, and plan-character counts. These are planning diagnostics,
not row counts or pure executor timings. Profiled runs must be compared with profiled runs; keep unprofiled regression
evidence separate.

Set `STRUCTURE_INTEGRATION_CHECKPOINT_TIMING=1` when checkpoint cost should be visible without enabling the complete
profiler. It wraps existing checkpoint calls and adds no Spark action.

### Repeated comparisons

Use alternating baseline/candidate repetitions and report individual values plus medians. For the Search boundary case,
`scripts/benchmark_search_boundaries.py` writes raw logs, parsed phase values, exit statuses, heap-failure flags, and
medians. The profiling runbook documents its command and output files.

## Compiler-level controls

### Shared batch-plan reuse

Use `plan_boundaries="auto"` to cut only shared batch subgraphs consumed by multiple downstream steps or outputs.
Use `"off"` to reproduce pre-optimization behavior and `"strict"` for diagnostics. The policy is backend-neutral,
but runtime boundaries are batch-only:

    if not frame.isStreaming:
        frame = apply_plan_boundary(frame, spark)

Streaming frames retain their original lineage and never receive compiler temporary-view boundaries.

### Guard and branch reuse

Prefer bounded Spark-side validation that examines no more data than required and reuses equivalent checks within one
transform run. Fuse a safe deterministic projection-union branch when it removes a repeated copy without changing row
multiplicity. Neither optimization is a substitute for a true query-plan boundary when the remaining graph still grows.

### Intermediate validation and stage exposure

Intermediate schema validation is an explicit diagnostic opt-in for Spark Connect, not the default performance path.
Focused tests should disable unused stage-output exposure. These switches must not remove final input/output validation
or strict user-facing safety checks.

### Compiled-artifact reuse

The PySpark integration support can reuse in-memory compiled transform artifacts between generated-source preparation
and runtime sessions. Use the module-scoped owner and `runtime_sessions` factory supplied by
`tests/integration/pyspark/conftest.py`; they share compiler metadata only, not Spark sessions, DataFrames, or results.
The default `STRUCTURE_COMPILED_ARTIFACT_REUSE=module` mode measures the warm lookup path. Use `off` for an isolated
comparison, and enable `STRUCTURE_PROFILE_COMPILATION=1` to report cold builds, warm hits, failures, and module totals.
Keep the generated package, plugin profile, validation settings, stage-output policy, schema registry, and code options
identical when comparing paths because those settings participate in compiler compatibility keys. Include source
preparation in total runtime, and do not add compiler, explain, or checkpoint subtimings a second time when they are
nested inside construction.

If module mode still reports misses for an apparently repeated transform, compare the phase, generated package,
execution policy, schema types, generated code options, source/dependency fingerprint, and non-default constructor
parameters. If a test uses separate owners or creates a fresh pool per session, the miss is expected. Close the
module-scoped owner at teardown; it retains compiler metadata only and must never be used as a Spark-frame or result
cache.

## Use cases

### Search integration

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

Keep `SearchDocuments` as the single public component. `FuseDocuments(materialize=~streaming)` owns the existing
conditional materialization so Search callers do not manage intermediate artifacts. Materialize the smallest reusable
candidate relation before reranking or uniqueness checks, while retaining the final feedback-branch checkpoint.

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
- Performance runbook: [Performance.runbook.md](../../runbooks/Performance.runbook.md)
- Profiling runbook: [Profiling.runbook.md](../../runbooks/Profiling.runbook.md)
