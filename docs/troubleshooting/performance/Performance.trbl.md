# PySpark Integration Performance Troubleshooting

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
