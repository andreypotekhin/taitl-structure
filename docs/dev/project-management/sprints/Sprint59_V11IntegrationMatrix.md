# Sprint 59: V11 Integration Matrix and Evidence

Status: planned; target: 2027-02-19.

## Sprint goal

Stage the PySpark 4.1 lane in local Compose and collect evidence for V11 work without running the earlier integration
suite on 4.1. The eventual matrix still includes ordinary and Connect lanes for 3.5, 4.0, and 4.1.

## User-facing outcome

Maintainers can run `make integration BACKEND=pyspark41`; that lane checks the runtime version and runs V11 tests only.
Connect 4.1 and the full regression selection remain follow-up work.

## Implementation tasks

- [x] Add the pinned ordinary PySpark 4.1 image, Spark services, runner choice, environment variables, and README commands.
- [x] Add the ordinary backend/profile mapping and runtime assertion; restrict the 4.1 runner to the runtime check and V11 tests.
- [x] Pin Protobuf 6.33.0 only for the 4.1 image and select RocksDB in 4.1 V11 test sessions for TransformWithState column families.
- [ ] Add Connect 4.1 and its dependency cache after its test selection and API support boundary are reviewed.
- [ ] Run the full six-lane matrix, generated-source checks, streaming checks, and regression tests before release.
- Record unavailable optional-provider or Connect evidence explicitly.

## Acceptance

The initial stage is complete when the ordinary 4.1 lane reports its pinned version/profile and selects only V11 tests.
Release acceptance still requires the full six-lane matrix and live evidence for every supported 4.1 feature. The image
build remains blocked by Docker VM storage exhaustion. A local PySpark/Spark 4.1.0 version check passed, but its host
Python 3.12 processor query exceeded the timeout, so the pinned Python 3.11 lane still needs live parity and restart
evidence.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`.
