# Sprint 59: V11 Integration Matrix and Evidence

Status: planned; target: 2027-02-19.

## Sprint goal

Stage the PySpark 4.1 lane in local Compose and collect evidence for V11 work. Ordinary 4.1 retains the V11-only
selection; Connect 4.1 runs the full integration and live concept suites after isolated Iceberg and Delta passes.
The eventual matrix still includes ordinary and Connect lanes for 3.5, 4.0, and 4.1.

## User-facing outcome

Maintainers can run `make integration BACKEND=pyspark41` for the version check and V11 tests, or
`make integration BACKEND=spark-connect41` for full Connect 4.1 integration and live concept coverage.

## Implementation tasks

- [x] Add the pinned ordinary PySpark 4.1 image, Spark services, runner choice, environment variables, and README commands.
- [x] Add the ordinary backend/profile mapping and runtime assertion; restrict the 4.1 runner to the runtime check and V11 tests.
- [x] Pin Protobuf 6.33.0 only for the 4.1 image and select RocksDB in 4.1 V11 test sessions for TransformWithState column families.
- [x] Add Connect 4.1 Delta infrastructure and retain its separate provider pass.
- [x] Expand Connect 4.1 to the full integration and live concept selection, excluding its isolated Delta and Iceberg
  modules from the general pass. The runner test records all three phases and verifies checkpoint-directory cleanup.
- [x] Use the mounted launcher on all six runners, explicitly build selected runner images, and verify source freshness,
  lazy startup, phase selection, cleanup, and failure propagation with 40 Spark-free cases.
- [x] Complete the fresh, unfiltered Connect 4.1 run and correct the earlier V11-only evidence scope.
- [x] Prove Connect 4.1 `Column.transform` online/generated parity and reliable batch checkpointing with a deep plan,
  preserved alias, and temporary-view cleanup.
- [x] Run the shared deep-plan checkpoint regression on Connect 4.0 and Connect 4.1; both preserve output behavior and
  remove their temporary views.
- [ ] Collect the row `transform_with_state` 4.1 matrix from its admission plan, and keep Pandas state and legacy
  `applyInPandasWithState` evidence as independent profile rows.
- [ ] Run the full six-lane matrix, generated-source checks, streaming checks, and regression tests before release.
- Record unavailable optional-provider or Connect evidence explicitly.

## Historical evidence (2026-10-07)

The pinned image used PySpark 4.1.0, Spark 4.1.0, Python 3.11.17, Delta 4.1.0, and Iceberg 1.12.0. Connect 4.1
reported Iceberg 15 passed; Delta 33 passed and one skipped (ordinary-only client-JVM metadata inspection). The cached
launcher selected V11 only and reported 6 passed and 27 skipped at the existing Connect, streaming, and state ownership
boundaries. This historical run does not establish full integration/concept coverage. Fresh-launcher evidence follows
below. The `Column.transform` test passed with nullable input, an inline callback, a bound `@special(type="expr")` helper that changes `long` to `string`, and online/generated schema/value parity. The focused checkpoint test passed.
Focused Spark-free runner, capability, source, checkpoint-gate, API ledger, and code-generation checks passed 379 tests
with 5 skips, then 361 focused checks passed after the exact-profile and classic-only source assertions were added.
That run's `make build` passed with 2,339 tests passed and 322 skipped in the main suite, 115 passed and 7 skipped in
the focused suite, successful formatting/Flake8/mypy, and both package artifacts built.

The ordinary 4.1 provider passes reported Iceberg 15 passed and Delta 31 passed / 3 skipped. Its V11 selection reported
32 passed and one failure in the active foreachBatch work: the assertion expects `AlertMessage` while the handoff carries
the declared `Alert` schema. The `Column.transform` case passed. That boundary is tracked in its owning chat; the files
were preserved here. State processor and foreachBatch coverage remain independent work and this evidence does not
close the full six-backend release matrix.

## Corrected full Connect evidence (2026-10-07)

The repository-mounted launcher reported checksum 3047266420 (9,184 bytes), matching the checked-in source path
used by all six Compose services. No test filters were supplied. Fresh collection and live collection agreed:

| Phase | Collected | Result | Time |
| --- | --- | --- | --- |
| Iceberg, isolated | 15 | 15 passed | 41.43s |
| Delta, isolated | 34 | 33 passed / 1 skipped | 110.96s |
| Integration and live concepts, excluding providers | 272 | 215 passed / 57 skipped | 334.18s |

Runtime pins: Python 3.11.17, PySpark/Spark 4.1.0, Delta 4.1.0, Iceberg 1.12.0, pandas 2.2.3, PyArrow 19.0.1,
Python Protobuf 6.33.0, and server Protobuf Java 4.33.0. The general suite includes the inline/bound callback case,
nullable inputs, Long-to-String output, schema/value parity, generated spelling, native checkpointing, and deep-plan
alias/view-cleanup parity.

General skip reasons: 34 ordinary-only grouped/state/processor cases; 15 ordinary-only streaming cases; six
caller-owned foreach/foreachBatch sink cases; one 3.5-specific CalendarInterval rejection case; one optional Sedona
provider case scoped by its V9 specification to Spark 3.5/4.0. Delta's single skip is ordinary-only client JVM metadata
inspection. The first full run exposed the missing Sedona SQL extension and a source assertion quoting mistake;
these were corrected and the full selection rerun. No unexpected batch failures became blanket skips.

Complete logs and fresh collection are retained under `.pytest-workspace-tmp/integration` as
`v11-connect41-full.log` (the failed diagnostic run), `v11-connect41-final.log` (the successful full run), and
`v11-connect41-collection.log`.

Shared Connect 4.0 regression selected 13 cases with the checkpoint/boundary/version filter: six passed, seven
ordinary-only streaming/state/sink cases skipped, and 308 were deselected in 29.80s. Its version check printed
PySpark/Spark 4.0.0. Ordinary 4.1 retained isolated providers and selected the callback/version cases: two passed,
31 were deselected, and the version check printed PySpark/Spark 4.1.0. Both use Python 3.11.17, pandas 2.2.3, and
PyArrow 19.0.1; Connect 4.0 uses Delta 4.0.1 / Python Protobuf 5.29.3, and ordinary 4.1 uses Delta 4.1.0 / Python
Protobuf 6.33.0. Their logs are `v11-connect40-regression.log` and `v11-pyspark41-callback.log`. All live runs were
sequential and retained shared services/caches. Focused Spark-free capability/source/runner checks passed 489 cases,
including 40 runner lifecycle cases. `make build` passed isort, Flake8, mypy (1,407 source files), 2,375 main tests
with 322 integration/optional skips, 151 focused tests with seven live-concept skips, and wheel/sdist creation. Main
and focused suites took 224.31s and 224.30s respectively. The complete build log is `v11-build.log`. No Connect
checkpoint directories or runner containers remain; all six pre-existing Spark services retain their uptime.

## Acceptance

The initial stage is complete when the ordinary 4.1 lane reports its pinned version/profile and selects only V11 tests.
Release acceptance still requires the full six-lane matrix and live evidence for every supported 4.1 feature. State
processor evidence is recorded per API family and output/time-mode matrix. Delta Connect evidence remains scoped to its
exact package and does not promote other Connect profiles. State processor and foreachBatch evidence is recorded in
its owning feature plans. The full release matrix remains open.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`,
`docs/dev/planning/past/P10072603.V11-connect-full-evidence.plan.md`,
`docs/dev/planning/past/P10062603.V11-transform-with-state-admission-and-typed-parity.plan.md`,
`docs/dev/planning/past/P10042604.V11-transform-with-state-in-pandas.plan.md`,
`docs/dev/planning/P10062602.V11-apply-in-pandas-with-state.plan.md`, and
`docs/dev/planning/past/P10062601.V11-delta-connect-admission.plan.md`.
