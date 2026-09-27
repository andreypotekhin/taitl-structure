# Structure Optimization

Use this workflow when a Structure transform or integration case needs to become faster or more memory-safe. It connects
the repeatable process to the detailed [Performance optimization guide](optimization/Performance.opt.md) and
[Memory optimization guide](optimization/Memory.opt.md).

## Choose the problem

Start by describing where the time or memory goes:

- **Before rows are returned:** investigate preparation, query-plan construction, serialization, and materialization.
- **While rows are being processed:** investigate Spark actions, joins, shuffles, skew, input files, and partitioning.
- **Driver heap exhaustion:** investigate repeated lazy lineage and the boundary between plan construction phases.
- **Online/generated difference:** separate cold setup and generated-source work from steady-state execution.

Do not treat every slow run as a memory problem, and do not treat a driver planning failure as an executor problem.

## Establish a baseline

Keep these values fixed for the baseline and every comparison:

- backend and Spark/PySpark version;
- driver and executor settings;
- input fixture and test selector;
- Structure configuration, including validation and `plan_boundaries` settings;
- execution mode and generated package; and
- cold or warm state.

Run the unchanged case first. Save its log, exit status, warnings, semantic results, schemas, and any heap failure.

## Measure the phases

Use the [Performance runbook](../runbooks/Performance.runbook.md) to classify the dominant phase and
select one control. Then use the [Profiling runbook](../runbooks/Profiling.runbook.md) for the focused Compose command
and timing-record template. It explains how to read `[phase]` and `[plan-profile]` lines, how checkpoint time nests
inside construction, and how to distinguish pytest time from full runner wall-clock time.

For policy comparisons, alternate baseline and candidate runs. Use at least three repetitions per policy and report both
individual values and medians. Keep profiled and unprofiled evidence separate because profiling changes timings.

## Select one control

For construction and execution-time problems, consult [Performance.opt.md](optimization/Performance.opt.md). Choose
one control that addresses the measured phase, such as reducing repeated validation, reusing a shared batch subgraph, or
changing a boundary policy.

For driver memory and expanding lazy lineage, consult [Memory.opt.md](optimization/Memory.opt.md). Choose
whether to:

- **diminish** repeated work with a semantics-preserving reduction;
- **bound** lineage with an explicit checkpoint at an approved reuse point; or
- **remove** the recurrence by redesigning it around a stable base relation.

Do not combine unrelated controls in the first comparison. A larger driver heap, cache, persist, alias, or temporary
view can change symptoms without proving that the plan is safe or smaller.

## Verify the result

After each change, compare the baseline and candidate on the same fixture. Verify:

- output rows, multiplicity, and ordering where ordering is part of the contract;
- schema, nullability, and validation failures;
- intentional UDF and strict-check behavior;
- online/generated parity; and
- cleanup of Structure-owned temporary artifacts.

A faster disconnected micro-test is not production evidence. Streaming frames require separate treatment because
batch-only compiler boundaries do not apply to actual streaming DataFrames.

## Report the evidence

Record the exact command, backend, runtime versions, settings, repetitions, individual timings, medians, warnings, heap
failures, semantic checks, and remaining limitations. State whether totals include Compose startup, checkpoint
execution, collection warm-up, and cleanup. Link the report to the relevant optimization guide and issue record.

## References

- [Profiling.runbook.md](../runbooks/Profiling.runbook.md)
- [Performance.runbook.md](../runbooks/Performance.runbook.md)
- [Memory.runbook.md](../runbooks/Memory.runbook.md)
- [Performance.opt.md](optimization/Performance.opt.md)
- [Memory.opt.md](optimization/Memory.opt.md)
- [Performance troubleshooting](../troubleshooting/performance/Performance.trbl.md)
- [Memory troubleshooting](../troubleshooting/memory/Memory.trbl.md)
