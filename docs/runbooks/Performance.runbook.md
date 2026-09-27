# PySpark Performance Runbook

Use this runbook when a Structure transform or integration test is slower than expected. It turns an observed delay
into phase-specific evidence before selecting an optimization. For the copy-paste profiling command and detailed log
field definitions, continue to the [Profiling runbook](Profiling.runbook.md).

Do not add Spark actions solely to investigate. `count()`, `collect()`, and `show()` can change the workload and move
the apparent bottleneck.

## 1. Classify the symptom

Start with the first point at which time accumulates:

- **Before rows are available:** investigate fixture/source preparation, Structure compilation, Spark plan construction,
  serialization, and materialization.
- **While rows are processed or collected:** investigate joins, shuffles, skew, partitions, input files, and result
  handling.
- **During cleanup:** investigate generated imports, temporary modules, temporary views, and fixture teardown.
- **With a driver heap failure:** follow the [Memory runbook](Memory.runbook.md) and classify the first failure before
  changing memory settings.

A small fixture does not prove that the workload is small. A compiler graph can expand a logical plan at joins,
unions, assertions, or fan-out branches even when only a few rows are collected.

## 2. Capture an unchanged baseline

Run the unchanged case from the repository root. Record these values before changing a setting:

- backend service and exact Spark/PySpark version;
- driver heap, executor settings, and relevant Spark configuration;
- test selector, fixture, input schema, and expected output shape;
- execution mode, generated package, validation settings, stage-output policy, and plan-boundary policy;
- cold or warm state, including generated-source and compiler-artifact state; and
- Pytest exit status, first meaningful warning or failure, and total elapsed-time definition.

For a focused integration reproduction, use the profiling runbook's command. The following settings provide phase and
plan evidence while preserving the test's normal actions:

```sh
set -o pipefail
runner_args="-k your_test_selector -vv -s --durations=20"
/usr/bin/time -p docker compose --env-file infra/compose/.env \
  -f infra/compose/docker-compose.yaml -p structure-integration run --rm \
  -e STRUCTURE_PROFILE_QUERY_PLANS=1 \
  -e STRUCTURE_INTEGRATION_CHECKPOINT_TIMING=1 \
  -e STRUCTURE_PLAN_BOUNDARIES=auto \
  -e STRUCTURE_INTEGRATION_TIMEOUT=600 \
  -e "INTEGRATION_PYTEST_ARGS=$runner_args" \
  structure-integration-pyspark35 \
  bash /workspace/infra/compose/images/pyspark/run-integration.sh pyspark35 \
  2>&1 | tee performance-investigation.log
run_status=${pipestatus[1]}
printf 'integration exit status: %s\n' "$run_status"
exit "$run_status"
```

Use the same backend, fixture, driver settings, and selector for every comparison. For Search, also set
`STRUCTURE_SEARCH_STAGE_OUTPUTS=0` when intermediate stage outputs are not part of the behavior under test.

## 3. Read the phase evidence

Use completed `[phase]` lines, not their `starting` lines. Record every matching label because phase names vary by
test:

- **Preparation:** generated-source setup, fixture setup, imports, and compiler requests.
- **Construction:** online/generated graph construction and Spark analysis.
- **Materialization:** checkpoint or snapshot work. This is normally nested inside construction.
- **Collection:** the Spark action after construction; compare online and generated collection separately.
- **Cleanup:** generated modules, temporary views, plan boundaries, and fixture teardown.

`[plan-profile]` output identifies guard timing, estimated plan expansion, checkpoint explanation, checkpoint duration,
and plan-character counts. These are structural diagnostics, not row counts, exact Catalyst node counts, or pure
executor timings. Compare profiled runs only with other profiled runs.

The Pytest `in ...s` value excludes some runner overhead. `/usr/bin/time`'s `real` value includes the full Compose
invocation. Choose one definition for the comparison and state it in the report; do not silently mix them.

## 4. Investigate the dominant phase

### Preparation or compilation dominates

Separate repeated Structure compilation from Spark work:

```text
STRUCTURE_COMPILED_ARTIFACT_REUSE=off
STRUCTURE_PROFILE_COMPILATION=1
```

Repeat with `STRUCTURE_COMPILED_ARTIFACT_REUSE=module` in a fresh runner process. `module` shares compiler metadata
between generated-source preparation and runtime sessions through the fixture-owned pool. `off` creates an isolated
pool for each request. Neither mode shares Spark sessions, DataFrames, or collected results.

Read `[compile]` records and the module total. A `misses=1` record built an artifact; a `hits=1` record found a
compatible artifact after key construction. The duration includes key construction and cache lookup. Include source
preparation in the total, but do not add compiler timings again when they are nested inside preparation or construction.

If module mode still misses for an apparently repeated request, compare the generated package, plugin profile, schema
types, generated code options, validation and stage-output policies, source/dependency fingerprints, and non-default
transform parameters. A new owner or pool also makes a miss expected. A cache hit does not shorten Spark lineage or
avoid Spark analysis, checkpoints, collection, or cleanup.

### Construction or plan analysis dominates

Enable the query-plan profiler and inspect expanded input references, guard work, checkpoint explanation, and plan
growth. Keep batch `plan_boundaries="auto"` as the control unless the investigation specifically compares boundary
policies. Use `off` to reproduce the unbounded control and `strict` for diagnostics. These boundaries are batch-only;
do not apply them to streaming frames.

If a relation is reused after joins, unions, projections, assertions, or fan-out branches, reduce the fixture while
preserving that operation shape. A smaller row count does not necessarily reduce the logical-plan expansion. For a
driver heap failure, use the [Memory runbook](Memory.runbook.md) to compare an approved checkpoint or materialization
boundary; do not call a cache, alias, or temporary view proof that the logical plan was shortened.

### Materialization dominates

Keep checkpoint explanation and checkpoint execution separate in the evidence, but remember that both are nested inside
construction. Move a boundary only when the resulting storage, recovery, streaming, and cleanup semantics are accepted.
Do not add checkpoints everywhere to make a timing line appear; preserve the smallest reusable relation and verify that
the final outputs remain unchanged.

### Collection or executor work dominates

Once rows are available, inspect the actual Spark workload: joins, shuffles, skew, partitioning, file sizes, UDFs, and
the number of rows returned. A Structure plan-boundary or compiler-cache change does not make an expensive executor
operation cheap. Keep the same input and result handling while testing Spark-level controls.

### Cleanup dominates

Inspect generated-module installation, temporary-view cleanup, plan-boundary cleanup, and fixture teardown. Include
cleanup in full-run totals, but do not attribute it to Spark construction without a matching phase record. Ensure that
module-scoped compiler owners are closed and that they retain compiler metadata only, never Spark frames or results.

## 5. Run a controlled comparison

Change one control at a time. For a baseline/candidate comparison:

1. Start a fresh runner process for each sample.
2. Alternate baseline and candidate order to limit startup and host-load bias.
3. Run at least three repetitions per policy.
4. Keep profiling settings identical across both policies.
5. Record individual values, medians, failures, heap errors, and semantic results.

For compiled-artifact reuse, compare `off` and `module`. For plan-boundary work, compare `off`, `auto`, or `strict` as
the specific experiment requires. Do not compare a warmed run with a cold run, or a different driver heap, fixture,
backend, generated package, or validation policy.

## 6. Verify semantics and report

Every candidate must preserve:

- output rows, multiplicity, ordering where contractual, and schema/nullability;
- strict validation failures and intentional UDF behavior;
- online/generated parity; and
- cleanup of Structure-owned temporary artifacts.

Copy this record into the issue or benchmark report:

```text
Issue / reproduction:
Backend / Spark / PySpark version:
Driver and executor settings:
Test selector / fixture / schema:
Structure policies:
Compiler-artifact mode and profiler:
Cold or warm state:
Preparation:
Online construction:
Generated construction:
Materialization/checkpoint (nested in construction):
Online collection:
Generated collection:
Cleanup:
Pytest total:
Wall-clock total:
Exit status / first meaningful failure:
Warnings / heap failures:
Individual runs and median:
Semantic and parity checks:
Remaining limitations:
```

Claim a performance improvement only when the measured total supports it. If compilation is eliminated but total time is
unchanged, report the remaining dominant phase rather than moving its nested timing into another category.

## References

- [Performance optimization methodology](../dev/optimization/Performance.opt.md)
- [Structure optimization workflow](../dev/Optimizing.md)
- [Profiling runbook](Profiling.runbook.md)
- [Memory investigation runbook](Memory.runbook.md)
- [Performance troubleshooting](../troubleshooting/performance/Performance.trbl.md)
- [Memory troubleshooting](../troubleshooting/memory/Memory.trbl.md)
- [Search integration performance issue](../dev/issues/I09272601.Search-integration-performance.issue.md)
