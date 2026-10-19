# Sprint 56: V11 Expression and Column Parity

Status: in progress; target: 2027-01-08. Column callbacks, the first five scalar helpers, and the TIME family are
implemented on the reviewed PySpark 4.1 profile.

## Sprint goal

Implement the approved typed PySpark 4.1 expression slice, including `Column.transform` where its callback contract is
admitted.

## User-facing outcome

Users can write supported 4.1 row-preserving expressions and receive the same values, schemas, diagnostics, and
generated code behavior in online and generated execution on admitted ordinary and Connect 4.1 profiles.

## Implementation tasks

- Extend expression IR, type/nullability inference, evaluator, renderer, imports, and capability keys.
- Add seeded/nondeterministic and streaming classifications.
- Add focused specification tests and ordinary 4.1 integration fixtures.
- Prove or gate Connect support per function family.

## Acceptance

Supported rows pass online/generated parity on ordinary 4.1; unsupported profiles fail with actionable capability
diagnostics; no random or arbitrary callback behavior is silently admitted.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md` and `docs/dev/design/V11PySpark41ExpressionParity.design.md`.

## Scalar helper evidence (2026-10-07)

Implemented `chr`, `quote`, `try_to_date`, `random`, and `uuid` through the shared expression recipe, evaluator,
renderer, public exports, and individual `expression.<name>` capability keys. Admission is exact `>=4.1,<4.2` for
ordinary and Connect variants; the default and unproven later profiles reject with `BACKEND-E2402`.

`chr` shares `char`'s integral input rules. `quote` matches Spark's always-nullable schema, even for required input.
`try_to_date` conservatively returns nullable Date regardless of source narrowing, accepts a literal pattern, and
returns null for malformed text in either ANSI mode. A format on Date/LTZ Timestamp input emits `PYSPARK-W2705`.
The date and string helpers are stateless and compatible with streaming by design; live streaming is unclaimed.
Random/UUID helpers reuse the explicit literal seed policy, carry nondeterminism metadata, and report `STREAM-E0801`
in streaming projections, filters, special expression bodies, and Column callbacks. Projection/union optimization
now respects the nondeterminism marker.

### Reproduction

Run these lanes sequentially. The filter keeps both provider phases while selecting the new helper and version tests:

```sh
for backend in spark-connect41 pyspark41; do
  docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm \
    -e 'INTEGRATION_PYTEST_ARGS=-k=(iceberg)or(delta)or(scalar_helpers)or(runtime_versions) -vv' \
    "structure-integration-${backend}" || exit
done
poetry run pytest tests/specifications/pyspark-code-generation/test_v11_scalar_helpers.py \
  tests/specifications/compatibility tests/specifications/streaming-compatibility
make build
```

Both images use Python 3.11.17, PySpark/Spark 4.1.0, Delta 4.1.0, Iceberg 1.12.0, pandas 2.2.3,
PyArrow 19.0.1, and Python Protobuf 6.33.0. Connect uses server Protobuf Java 4.33.0. The mounted launcher identity is
`3047266420`, 9,184 bytes.

| Lane | Iceberg | Delta | Helper/version selection |
| --- | --- | --- | --- |
| Connect 4.1 | 15 passed, 40.97s | 33 passed / 1 skipped, 110.35s | 7 passed, 8.47s; 278 collected / 271 deselected |
| Ordinary 4.1 | 15 passed, 70.13s | 31 passed / 3 skipped, 199.26s | 7 passed, 32.96s; 39 collected / 32 deselected |

The Connect Delta skip retains the existing ordinary-only JVM metadata case. Ordinary Delta skips two
Connect-specific declaration-drift cases and one remote-request-count case. The helper selections have no skips.
It proves nulls, empty and escaped strings, non-ASCII text, negative/modulo/NUL character values, leap-day and malformed
date input, Date/LTZ conversion, nullable schemas, generated spelling, and no classic-only access in generated source.
Random tests prove seeded native/online/generated equality with one and two partitions; unseeded tests check ranges,
UUID shape, row counts, and non-null schemas without claiming equal random values.

The diagnostic run first passed all values but exposed Spark's nullable `quote` schema. The constructor and
projection nullability rules were corrected; the native schema probe and the successful rerun retain the evidence.
An initial filter omitted providers and correctly exited 5 with no selected Iceberg tests; the recorded command fixes
the filter by retaining provider names. Gateway/checkpoint cleanup ran on both failed attempts and the successful run.

Spark-free evidence: 80 focused helper checks pass, including unsupported profiles, literal rules, compiler
nullability, streaming rejection, and nondeterminism. The broader compatibility/streaming selection passed 265 checks
before the additional required-output and nested-recipe checks. `make build` passed isort, Flake8, mypy (1,409 files),
the main suite (2,455 passed / 328 skipped, 303.39s), the final golden/differential/metamorphic/concepts/compatibility
selection (151 passed / 7 skipped, 132.99s), and the 0.1.1 sdist/wheel builds. The main suite collected 2,783 tests;
its 328 skips include the six new live cases excluded by the default Spark-free run. Live runners exited successfully
and preserved the six existing Spark services. After the evidence metadata update, 117 helper/delta/catalog guards
passed in 0.70s.

Logs are retained under `.pytest-workspace-tmp/integration/` as `v11-helpers-connect41.log`,
`v11-helpers-connect41-diagnostic.log`, `v11-helper-nullability-probe.log`, `v11-helpers-pyspark41.log`,
`v11-helper-static.log`, `v11-helper-nesting.log`, `v11-helper-final-guards.log`, and `v11-helpers-build.log`.

## TIME helper evidence (2026-10-07)

Implemented public `time(precision=6)`, `Time`/`TimeType`, and the six expression helpers. Precision 0–6 survives
schema identity, nested rendering/materialization, and schema reads. Python `datetime.time` literals retain their
clock fields when timezone-aware; generated code now imports `datetime` for those literals. TIME comparisons require
matching precision; ordering accepts all TIME precisions. TIME/String casts and TIME-to-TIME precision reduction
preserve Spark behavior. Spark's native
feature flag remains caller-configured; disabling TIME surfaces Spark's own error. Strict `to_time` errors and
`try_to_time` null behavior are verified with ANSI both on and off. No streaming claim is made.

Pinned runtime: Python 3.11.17, PySpark/Spark 4.1.0, Delta 4.1.0, pandas 2.2.3, PyArrow 19.0.1, Protobuf 6.33.0.
The mounted Connect launcher checksum is `3047266420 9184` for the full filtered run; its isolated Iceberg phase
passed 15 tests and Delta passed 33 with one existing ordinary-only JVM metadata skip. Final focused selections each
passed five tests: Connect in 12.83s and ordinary PySpark in 27.58s. They cover runtime versions, online/generated/native
values and schemas, dynamic format/unit expressions,
precision casts, matching-precision comparisons, Python time literals, current-time stability within a query, and
malformed parsing in both ANSI modes. Disabling TIME surfaces Spark's native SQL type error. Full filtered launcher
runs retained passing Iceberg and Delta phases (15 passed; 33 passed/1 skipped), while their general phases exposed
fixture-only type/nullability and cast-expectation mismatches. Those fixtures were corrected before the successful
focused runs.

Spark-free checks pass 147 focused helper tests, including exact-profile rejection, type/nullability rules, comparison
and ordering, cast boundaries, generated TIME spelling, and scans for classic-only Connect access. The default profile
remains `>=3.5,<4.1`.

Final `make build` evidence (2026-10-08): isort, Flake8, and mypy (1,413 source files) passed; the main pytest suite
passed 2,522 tests with 332 skipped in 193.56s; the additional golden/differential/metamorphic/concepts/API snapshot/
compatibility selection passed 151 with 7 skipped in 106.07s; and the 0.1.1 sdist and wheel built successfully.
`git diff --check` also passed. The live focused ordinary and Connect 4.1 TIME lanes are recorded above; broader
unfiltered Connect integration remains part of the separately tracked Connect parity work.

### Next discussion

Resolve the relational query API contract before implementation: whether `exists` and `isin(DataFrame)` use explicit
named key pairs or a relation predicate DSL; the behavior for nullable correlation keys and SQL three-valued `IN`
semantics; how aliases and accidental outer-column capture are resolved; and the first supported lateral join
cardinality/join-kind combination. The state processor and foreachBatch work remains owned by its existing chats.
