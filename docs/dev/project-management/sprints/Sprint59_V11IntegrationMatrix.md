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
- [x] Prove Connect 4.1 `Column.transform` online/generated parity and reliable batch checkpointing with a deep plan,
  preserved alias, and temporary-view cleanup.
- [x] Run the shared deep-plan checkpoint regression on Connect 4.0 and Connect 4.1; both preserve output behavior and
  remove their temporary views.
- [ ] Collect the row `transform_with_state` 4.1 matrix from its admission plan, and keep Pandas state and legacy
  `applyInPandasWithState` evidence as independent profile rows.
- [ ] Run the full six-lane matrix, generated-source checks, streaming checks, and regression tests before release.
- Record unavailable optional-provider or Connect evidence explicitly.

## Evidence (2026-10-07)

The pinned image used PySpark 4.1.0, Spark 4.1.0, Python 3.11.17, Delta 4.1.0, and Iceberg 1.12.0. Connect 4.1
reported Iceberg 15 passed; Delta 33 passed and one skipped (ordinary-only client-JVM metadata inspection); the general
integration/concept selection reported 6 passed and 27 skipped at the existing Connect, streaming, and state ownership
boundaries. The `Column.transform` test passed with nullable input, an inline callback, a bound `@special(type="expr")`
helper that changes `long` to `string`, and online/generated schema/value parity. The focused checkpoint test passed.
Focused Spark-free runner, capability, source, checkpoint-gate, API ledger, and code-generation checks passed 379 tests
with 5 skips, then 361 focused checks passed after the exact-profile and classic-only source assertions were added.
The final `make build` passed with 2,339 tests passed and 322 skipped in the main suite, 115 passed and 7 skipped in
the focused suite, successful formatting/Flake8/mypy, and both package artifacts built.

The ordinary 4.1 provider passes reported Iceberg 15 passed and Delta 31 passed / 3 skipped. Its V11 selection reported
32 passed and one failure in the active foreachBatch work: the assertion expects `AlertMessage` while the handoff carries
the declared `Alert` schema. The `Column.transform` case passed. That boundary is tracked in its owning chat; the files
were preserved here. State processor and foreachBatch coverage remain independent work and this evidence does not
close the full six-backend release matrix.

## Acceptance

The initial stage is complete when the ordinary 4.1 lane reports its pinned version/profile and selects only V11 tests.
Release acceptance still requires the full six-lane matrix and live evidence for every supported 4.1 feature. State
processor evidence is recorded per API family and output/time-mode matrix. Delta Connect evidence remains scoped to its
exact package and does not promote other Connect profiles. The ordinary 4.1 V11 run above retains one unrelated
foreachBatch failure; the full release matrix remains open.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`,
`docs/dev/planning/past/P10062603.V11-transform-with-state-admission-and-typed-parity.plan.md`,
`docs/dev/planning/past/P10042604.V11-transform-with-state-in-pandas.plan.md`,
`docs/dev/planning/P10062602.V11-apply-in-pandas-with-state.plan.md`, and
`docs/dev/planning/past/P10062601.V11-delta-connect-admission.plan.md`.
