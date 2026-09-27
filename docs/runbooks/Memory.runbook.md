# PySpark Memory Investigation Runbook

Use this runbook when a Structure transform is slow during query construction or the Spark driver runs out of heap.
It makes the minimum evidence in [Memory.opt.md](../dev/optimization/Memory.opt.md) concrete and keeps driver, executor,
and query-plan symptoms separate.

## Capture the execution context

Write these values down before changing the graph or increasing memory:

- **Backend:** the Compose service and backend argument, such as `structure-integration-pyspark35` and `pyspark35`.
- **Spark/PySpark version:** the version selected by the backend image. For a precise local check, run
  `python -c "import pyspark; print(pyspark.__version__)"` in the same environment as the failing test.
- **Driver heap:** `STRUCTURE_SPARK_DRIVER_MEMORY` for ordinary PySpark, or
  `STRUCTURE_SPARK_CONNECT_DRIVER_MEMORY` for Spark Connect. If neither is set, record `default` rather than guessing
  the JVM heap.
- **Executor settings:** master URL, worker count, executor memory, partition settings, and any custom Spark
  configuration. Record `default` for values you did not override.
- **Input and selector:** the fixture size, transform, test selector, execution mode, and whether the run is cold or
  warm.

The backend name identifies the supported Spark line, but it does not replace the exact version or memory settings in
the report.

## Run one bounded reproduction

Run the failing case from the repository root and save the complete output. Replace `your_test_selector`, the backend,
and the driver setting only when those are part of the case being investigated.

```sh
set -o pipefail
runner_args="-k your_test_selector -vv -s --durations=20"
/usr/bin/time -p docker compose --env-file infra/compose/.env \
  -f infra/compose/docker-compose.yaml -p structure-integration run --rm \
  -e STRUCTURE_PROFILE_QUERY_PLANS=1 \
  -e STRUCTURE_INTEGRATION_CHECKPOINT_TIMING=1 \
  -e STRUCTURE_INTEGRATION_TIMEOUT=600 \
  -e "INTEGRATION_PYTEST_ARGS=$runner_args" \
  structure-integration-pyspark35 \
  bash /workspace/infra/compose/images/pyspark/run-integration.sh pyspark35 \
  2>&1 | tee memory-investigation.log
run_status=${pipestatus[1]}
printf 'integration exit status: %s\n' "$run_status"
exit "$run_status"
```

The command's `real` value is the full wall-clock elapsed time. The Pytest final `in ...s` value excludes some Compose
overhead. Use the same definition for every comparison and record which one you selected.

If the suspected growth is in Structure preparation rather than Spark plan analysis, record the compiled-artifact mode
and profile it separately. Compare fresh runs with `STRUCTURE_COMPILED_ARTIFACT_REUSE=module` and `off`, and enable
`STRUCTURE_PROFILE_COMPILATION=1`. The pool contains compiler metadata only; it does not retain Spark DataFrames,
sessions, or results. A hit can reduce repeated compiler work but cannot shorten Spark lineage or fix a driver-side
Catalyst heap failure. Close the module-scoped owner after the test and keep this comparison separate from executor or
driver heap conclusions.

Do not add `count()`, `collect()`, or `show()` calls solely to measure memory behavior. They can introduce actions and
change the failure phase. The profiler reports plan explanation, checkpoint work, and structural expansion without
adding those actions.

## Identify the owner and first failure

Read the log from the beginning and record the first meaningful failure, not the secondary errors that follow a failed
Spark JVM. Use this classification:

- **Driver:** `java.lang.OutOfMemoryError: Java heap space` while Spark analyzes, explains, serializes, or constructs a
  plan; long delay before executor tasks start; or Spark Connect failing while sending a deep plan.
- **Executor:** executor JVM out-of-memory messages, task failures, shuffle spill or fetch failures, or failure while
  processing rows after the plan is constructed.
- **Application:** validation, schema, Python, or generated-import failures unrelated to Spark memory.

Record whether executor tasks were active when the first failure occurred. Increasing executor memory does not fix a
driver planning failure, and increasing driver heap does not fix an executor failure.

## Measure the phases

Read completed `[phase]` lines rather than their `starting` lines. Record:

- preparation and compilation;
- query-plan construction or explanation;
- checkpoint or other materialization work;
- row collection; and
- cleanup.

Checkpoint timings are nested inside construction. The `[plan-profile] checkpoint explain` and subsequent checkpoint
duration must not be added to the construction or total a second time. For the complete interpretation of phase output,
see the [Profiling runbook](Profiling.runbook.md).

## Minimize without changing the cause

Reduce the input to a few rows while preserving the operation shape that reuses the expanded relation. Keep the same
joins, projections, unions, assertions, and fan-out branches. If the failure survives the reduction, input cardinality
is not the controlling variable.

Compare one control at a time:

- a semantics-preserving algebraic reduction;
- `checkpoint()` or `local_checkpoint()` at an approved reuse point; and
- a temporary view, alias, `cache()`, or `persist()` as a negative comparison.

Do not call a shorter view name or a successful cache request proof that the logical plan was cut.

Choose one remedy according to the measured cause:

- **Diminish** repeated work with a semantics-preserving algebraic reduction.
- **Bound** lazy lineage with `checkpoint()` or `local_checkpoint()` at an approved reuse point.
- **Remove** the recurrence by redesigning the operation around a stable base relation.

Keep the first comparison to one remedy. A larger driver heap, cache, persist, alias, or temporary view can change the
symptom without proving that the logical plan is smaller or safe. Preserve the operation shape and verify rows, schema,
parity, and cleanup after each change.

## Evidence record

Copy this record into the issue or benchmark report:

```text
Backend / Compose service:
Spark / PySpark version:
Driver heap:
Executor settings:
Input / test selector / execution mode:
Cold or warm run:
Elapsed time definition and value:
Preparation / compilation:
Plan construction / explanation:
Checkpoint / materialization:
Collection:
Cleanup:
First meaningful failure:
Driver, executor, or application owner:
Executor tasks active at failure:
Plan-growth measurements:
Control compared:
Rows / schema / parity result:
```

Use the [Performance runbook](Performance.runbook.md) when the issue also includes a broader performance comparison.
