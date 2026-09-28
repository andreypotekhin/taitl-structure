# PySpark Performance Profiling

This is the executable runbook for recording PySpark integration timings. First use the
[Performance runbook](Performance.runbook.md) to classify the slow phase and choose the comparison;
then use this runbook with [Performance.opt.md](../dev/optimization/Performance.opt.md) to capture timings.

The repository already measures the important phases. Do not add Spark actions just to obtain timings: an action can
change the workload being measured.

## What to hold constant

Before comparing runs, write down:

- backend and Spark/PySpark version;
- driver heap and other driver settings;
- test selector and fixture;
- Structure settings, including `STRUCTURE_PLAN_BOUNDARIES`, stage-output exposure, and validation options;
- whether query-plan profiling is enabled; and
- whether the run is cold or warm.

Change one policy at a time. For a baseline/candidate comparison, alternate the policies so that startup and machine
load do not systematically favor one side. Run at least three repetitions of each policy.

## Single focused run

Run this from the repository root. Replace the test selector and backend together when profiling another case.

```sh
set -o pipefail
runner_args="-k test_document_search_reranks_bm25_candidates_for_multiple_queries -vv -s --durations=20"
/usr/bin/time -p docker compose --env-file infra/compose/.env \
  -f infra/compose/docker-compose.yaml -p structure-integration run --rm \
  -e STRUCTURE_PROFILE_QUERY_PLANS=timing \
  -e STRUCTURE_INTEGRATION_CHECKPOINT_TIMING=1 \
  -e STRUCTURE_PLAN_BOUNDARIES=auto \
  -e STRUCTURE_SEARCH_STAGE_OUTPUTS=0 \
  -e STRUCTURE_INTEGRATION_TIMEOUT=600 \
  -e "INTEGRATION_PYTEST_ARGS=$runner_args" \
  structure-integration-pyspark35 \
  bash /workspace/infra/compose/images/pyspark/run-integration.sh pyspark35 \
  2>&1 | tee performance-rerank.log
run_status=${pipestatus[1]}
printf 'integration exit status: %s\n' "$run_status"
exit "$run_status"
```

The command records both the full runner log and the shell's wall-clock output. In zsh, `pipestatus[1]` is the exit
status of Docker rather than `tee`; in another shell, use that shell's pipe-status facility or run without `tee`.

For a non-Search test, remove the Search-only `STRUCTURE_SEARCH_STAGE_OUTPUTS` setting. Keep the same backend, fixture,
driver settings, and test selector when comparing policies.

## Compiled-artifact reuse

Use the compiled-artifact switch when the question is whether generated-source preparation and runtime sessions repeat
the same Structure compilation. Run each policy in a fresh Compose process with the same test selector and all other
settings fixed:

```sh
-e STRUCTURE_COMPILED_ARTIFACT_REUSE=off
-e STRUCTURE_PROFILE_COMPILATION=1
```

Repeat with `STRUCTURE_COMPILED_ARTIFACT_REUSE=module`. `module` is the default and shares a fixture-owned pool across
generated-source preparation and runtime sessions within one integration module. `off` creates an isolated pool for
each request and is the control condition. Neither setting shares Spark sessions, DataFrames, or collected results.

Read `[compile]` records and the `module total` line. Each record includes the complete cache request, including key
construction, and reports `hits`, `misses`, and failures. Count a miss as avoided work only when the corresponding
module run records a hit; a module hit does not remove Spark DataFrame construction, analysis, checkpoint, or
collection work. Include source preparation and the module total in the report without adding nested phase values a
second time.

If a repeated request misses in module mode, compare the generated package, plugin profile, schema types, generated code
options, stage-output and validation policies, source/dependency fingerprints, and non-default transform parameters.
Different settings are intentionally different compiler keys. Also check that the test uses one module-scoped owner;
a new owner or pool per session makes the miss expected. A failed request is re-raised and does not establish a valid
artifact, so record the failure and the subsequent retry separately.

## Reading the log

Use the following output as the source for the timing record:

- **Preparation:** Read `[phase] generated source setup ...` and related setup/compile output. This covers fixture,
  source, and generated-artifact preparation.
- **Online construction:** Read `[phase] ... online construction: Ns`. This is the online execution path's build and
  analysis time.
- **Generated construction:** Read `[phase] ... generated construction: Ns`. This is the generated execution path's
  build and analysis time.
- **Checkpoint:** Read `[plan-profile] checkpoint after explain ...` or `[phase] DataFrame checkpoint ...`. This work
  is nested inside construction; do not add it twice.
- **Collection:** Read `[phase] ... online collection: Ns` and the corresponding generated collection line. This is
  Spark action time after construction.
- **Cleanup:** Read `[phase] ... cleanup: Ns` and `plan boundary cleanup`. This covers generated modules, temporary
  files, and Structure-owned plan boundaries.
- **Total:** Read Pytest's final `in ...s` line and `/usr/bin/time`'s `real` line. Pytest excludes some runner
  overhead; `real` includes the full Compose invocation. Record which one you use.
- **Exit status:** Read `integration exit status: N`. `0` means the selected test passed; any other value is a failed
  or interrupted run.

Phase labels vary by test. Record every matching phase rather than assuming that one test's labels exist in another.
The phase timer prints a `starting` line followed by the completed duration; record only the completed duration.

## Query-plan profiling

`STRUCTURE_PROFILE_QUERY_PLANS=timing` adds:

- guard and singleton-policy timings;
- estimated expanded input references around joins, assertions, and checkpoints;
- checkpoint elapsed time; and
- success or failure, checkpoint sequence, execution mode, and compiled-step attribution when available.

Timing mode does not call `explain`, collect, count, inspect schema, or fetch a query plan. Set the switch to `explain`
(with `1` retained as an alias) to add `explain(mode="simple")` time and plan-character count before each checkpoint.
These measurements describe planning and structural risk. They are not row counts, exact Catalyst node counts, pure
executor time, or file-writing time. Profiling changes warm-up and timing, so compare profiled runs only with other
profiled runs. Keep a separate unprofiled run for final regression evidence. Unset, `0`, and `off` disable the
profiler; other values fail before Spark is requested.

`STRUCTURE_INTEGRATION_CHECKPOINT_TIMING=1` times the existing DataFrame checkpoint calls without adding a Spark action.
Those times are nested within construction. Use it when checkpoint cost needs to be visible even without the full plan
profiler.

## Repeated policy comparison

For the Search boundary comparison, use the repository's benchmark helper. It alternates `off` and `auto`, writes one
log per run, parses phase output, and writes medians and individual results:

```sh
poetry run python scripts/benchmark_search_boundaries.py \
  --backends pyspark35 \
  --cases rerank \
  --repetitions 3 \
  --timeout 600 \
  --output performance-evidence/search-rerank
```

The output directory must not already contain logs from the same run; the helper refuses to overwrite evidence. Inspect
`results.json` for individual runs and `medians.json` for policy summaries. Treat failed runs and heap exhaustion as
evidence, not as missing data.

For another backend or case, use the choices shown by:

```sh
poetry run python scripts/benchmark_search_boundaries.py --help
```

The helper does not enable `STRUCTURE_PROFILE_QUERY_PLANS`. Use the single-run command when structural profiler output
is required, and keep the profiling setting identical across any runs being compared.

For the lexical gap-selection remedy, inspect the focused `[plan-profile]` records and the compiled final selection.
The production batch path unions the four already-deduplicated `ScoreQueryAvailability` lanes before one existence
join to `SearchQuery`. The expected structural result is one existence join instead of four final outer joins; the
focused semantic check must still cover row multiplicity, schemas, rankings, nulls, validation failures, and
online/generated parity.

The remedy does not authorize sharing the stored and streamed retrieval enrichment branches. That optimization remains
deferred until stage aliases and mixed batch/stream behavior are validated. Record streaming results separately from
the batch evidence, and keep the original path if the streaming contract is not proven.

## Timing record

Copy this compact record into the issue or benchmark report:

```text
Backend / Spark:
Driver settings:
Test selector / fixture:
Structure policies:
Profiling enabled:
Run temperature:
Preparation:
Online construction:
Generated construction:
Checkpoint/materialization (nested in construction):
Online collection:
Generated collection:
Cleanup:
Pytest total:
Wall-clock total:
Exit status:
Warnings / heap failures:
Semantic and parity checks:
```

For interpretation and optimization choices, return to
[Performance.opt.md](../dev/optimization/Performance.opt.md). For a short user-facing entry, see
[Performance.trbl.md](../troubleshooting/performance/Performance.trbl.md).
