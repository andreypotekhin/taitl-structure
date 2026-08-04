# Sprint 59: V11 Integration Matrix and Evidence

Status: planned; target: 2027-02-19.

## Sprint goal

Extend local Compose and test infrastructure from four to six version/variant lanes and collect positive and negative
PySpark 4.1 evidence.

## User-facing outcome

Maintainers can run `make integration BACKEND=pyspark41`, `make integration BACKEND=spark-connect41`, or the complete
`make integration` matrix and see exact runtime-version assertions.

## Implementation tasks

- Add pinned PySpark 4.1 image, Spark services, Connect cache, runner choices, environment variables, and README commands.
- Add backend/profile/variant matrix helpers and runtime assertions.
- Run all six lanes, generated-source checks, streaming checks, and regression tests.
- Record unavailable optional-provider or Connect evidence explicitly.

## Acceptance

All six backend names are selectable, each lane reports the correct version/profile/variant, and the supported 4.1
features have live ordinary evidence.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`.
