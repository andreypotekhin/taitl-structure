# Sprint 56: V11 Expression and Column Parity

Status: planned; target: 2027-01-08.

## Sprint goal

Implement the approved typed PySpark 4.1 expression slice, including `Column.transform` where its callback contract is
admitted.

## User-facing outcome

Users can write supported 4.1 row-preserving expressions and receive the same values, schemas, diagnostics, and
generated code behavior online and in ordinary PySpark 4.1.

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
