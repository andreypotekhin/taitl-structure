# Performance Troubleshooting

This page is for people using Structure to run PySpark transforms. It focuses on what you can check and change as the
caller of a transform; detailed profiling and compiler-development guidance belongs in the runbooks.

## Main references

- [Performance runbook](../../runbooks/Performance.runbook.md)
- [Profiling runbook](../../runbooks/Profiling.runbook.md)

## Symptoms

A transform can still be slow with a small input when:

- Spark spends a long time preparing the query before returning rows;
- the same relation is reused after joins, unions, or other expanding operations;
- the transform returns rows, but processing or collecting them is slow.

Slow query preparation and slow row processing are different problems. Use the use case below that best matches what
you observe.

## First response

1. Run the same transform with the same input and Spark settings so that you can tell whether a change helped.
2. Avoid adding extra `count()`, `collect()`, or `show()` calls just to investigate; each call can add Spark work.
3. Check whether the delay occurs before rows are available or while Spark is processing rows.
4. If a maintainer asks for timings, use the [profiling runbook](../../runbooks/Profiling.runbook.md) rather than
   estimating times from the total wait.

## Common remedies

### Unused steps are still built

Structure removes an unused step only when its plugin certifies that removal is safe. Check the explain report's
"unused steps" section: it gives original/retained counts and the reason for each decision. A branch can remain
because callers can inspect its declared stage output, or because it contains validation, a hook, a UDF, or explicit
resource lifecycle work. Unknown operations remain conservatively.

If you only need final outputs, set `allow_stage_outputs=False`; its default remains true for inspection convenience.
Use `prune_unused_steps=False` to compare with pruning disabled. Keep all other settings identical. Do not disable
validation merely to reduce the retained count: checks may be the reason an apparently unused branch is necessary.
Plugins without optimization support are unchanged. See [configuration](../../Configuration.md#unused-step-optimization)
and [the active compiler plan](../../dev/planning/P09272603.Unused-branch-optimization.plan.md).

- Use only the final outputs you need. Avoid materializing or collecting intermediate relations unnecessarily.
- Keep the default batch setting `plan_boundaries = "auto"`. It can reduce repeated plan analysis for shared batch
  subgraphs, but it does not materialize rows or truncate Spark lineage.
- Add an explicit `checkpoint()` or `local_checkpoint()` boundary before reusing a large relation when your storage
  and recovery requirements allow it.
- Treat `cache()`, `persist()`, Python aliases, and temporary views as reuse or naming tools, not as proof that the
  logical plan is smaller.
- Keep streaming data on streaming-compatible paths. Batch plan boundaries do not apply to actual streaming frames.
- If preparation repeats Structure compilation, compare the default module-owned artifact reuse with
  `STRUCTURE_COMPILED_ARTIFACT_REUSE=off` and `STRUCTURE_PROFILE_COMPILATION=1`. This changes compiler metadata reuse,
  not Spark execution.
- For checkpoint elapsed time without extra query-plan work, use `STRUCTURE_PROFILE_QUERY_PLANS=timing`. Use
  `STRUCTURE_PROFILE_QUERY_PLANS=explain` only for explicit query-plan diagnosis; explanation and checkpoint elapsed
  time are nested in construction and are not pure file-writing or executor times.

### Search repeats work while selecting lexical gaps

Use the Search lexical gap-selection implementation that keeps the four gap-detection lanes, unions their already
deduplicated `ScoreQueryAvailability` IDs, and matches `SearchQuery` with one existence join. This removes the four
final outer joins without changing freshness, scope, model, dimension, policy, optional-input, or duplicate-query
semantics. Verify the compiled plan has one existence join and compare rows and schemas in both online and generated
batch execution before accepting a custom equivalent.

Do not combine the stored and streamed retrieval enrichment branches as a follow-up remedy yet. That change requires a
stage-alias capability and explicit mixed batch/stream validation; applying it unconditionally can change visible stage
outputs or streaming support. Keep the existing retrieval path until those constraints are proven.

#### References

- [Search checkpoint-work evidence](../../dev/issues/I09272601/Search-checkpoint-work-reduction.evidence.md)
- [Performance runbook](../../runbooks/Performance.runbook.md#search-checkpoint-work-remedy)
- [Search integration performance issue](../../dev/issues/I09272601.Search-integration-performance.issue.md)

## Use cases

### The transform is slow before it returns rows

This usually means Spark is constructing or analyzing a large logical plan. It is common when a relation is reused by
multiple downstream branches. Keep `plan_boundaries = "auto"` for batch work and add an explicit checkpoint only at a
boundary whose storage, recovery, and evaluation behavior are acceptable.

#### References

- [Structure configuration](../../Configuration.md#validation-related-settings)

### The transform is slow after rows start processing

If rows are available quickly but the action remains slow, query planning may not be the bottleneck. Inspect the Spark
workload: joins, shuffles, skew, input file sizes, partitioning, and the number of rows being collected. Optimize the
input and Spark operation that dominates; a Structure plan boundary will not make an expensive executor operation cheap.

### Online and generated execution have different timings

Run both modes against the same input and compare cold and warm runs separately. Generated execution can include source
setup and import work on its first run; online execution can have a different warm-up profile. Keep the comparison
focused on the same final rows and schema. A timing difference is not a semantic difference, but changed rows or schema
requires investigation before treating the faster mode as an acceptable replacement.

### Preparation repeatedly compiles the same transform

This is a Structure compiler-cache question when profiling shows repeated `misses` for equivalent source-preparation or
runtime requests while Spark work is not yet dominant. Run the same case in fresh processes with
`STRUCTURE_COMPILED_ARTIFACT_REUSE=module` and `off`, keeping the generated package, plugin, schema types, validation
and stage-output policies, generated code options, and inputs fixed. Enable `STRUCTURE_PROFILE_COMPILATION=1` and record
the `[compile]` records, module total, and the normal phase timings.

If module mode still misses, check for a new pool or owner first, then compare the compiler settings and source/dependency
fingerprints. A changed setting or transform parameter is an intentional cache-key difference. A hit only proves that
Structure compilation was reused; it does not remove Spark plan construction, checkpoints, collection, or cleanup.
Include source preparation in the total and do not sum nested compiler timings twice.

#### References

- [Compiled-artifact profiling](../../runbooks/Profiling.runbook.md#compiled-artifact-reuse)
- [Performance runbook](../../runbooks/Performance.runbook.md#preparation-or-compilation-dominates)

### Compilation fails before the test starts

An error such as `union_all(relation) requires a Structure relation parameter or transform input` is a DSL binding
failure during source preparation. It is not evidence that Spark execution, plan construction, or compiled-artifact
reuse is slow. Check the failing transform helper first: relation-set operations must receive a declared Structure
relation input, not a row-scoped value created inside another step. For internal `lane(...)` outputs, use the
lane-compatible join/filter form or promote the relation to a declared transform input.

Correct the transform signature or pass the declared input relation directly, then rerun the same focused selector.
Keep the failed log and exit status, but exclude that run from timing medians. Only compare `off` and `module` after
the unchanged case completes successfully; changing reuse policy cannot repair an invalid DSL binding.

#### References

- [Search integration compile-setup gotcha](search_integration_slow.gotcha.md#compile-setup-failure)
- [Performance runbook](../../runbooks/Performance.runbook.md#compile-setup-fails)

### A streaming transform is slow or ignores plan boundaries

Actual streaming DataFrames bypass Structure's batch plan boundaries. Check the source, trigger, state, sink, and
checkpoint location owned by your streaming application. Do not add a batch checkpoint or a repeated action merely to
force a boundary; use a streaming-compatible design and measure the end-to-end query.

#### References

- [Integration guidance](../../Integration.md)
- [Structure configuration](../../Configuration.md#validation-related-settings)

### A maintainer asks for a reproducible timing report

Use the [Performance runbook](../../runbooks/Performance.runbook.md) to classify the bottleneck and choose
one control, then use the profiling runbook's focused command and timing template. Record the backend, Spark version,
driver settings, input, Structure policies, phase timings, total time, warnings, and exit status. Keep the same fixture
and settings for the baseline and candidate; run at least three alternating repetitions when comparing policies.

#### References

- [Search slow-fixture gotcha](search_integration_slow.gotcha.md)

## Related

Memory-specific symptoms belong in the public memory troubleshooting flow:

- [Memory troubleshooting](../memory/Memory.trbl.md)
- [Memory runbook](../../runbooks/Memory.runbook.md)
- [Driver-heap gotcha](../memory/spark_driver_heap_oom.gotcha.md)
