# Sprint 12: v3 Join PySpark Parity Hardening

## Sprint Goal

Close the planned join gaps from `docs/dev/Gaps.md` while preserving explicit join semantics, typed projections, and
capability-checked generated PySpark.

## Product Outcome

Developers can use using-key joins, receive sharper right/full diagnostics, request cross joins safely, use supported
join strategy directives, and express forward as-of joins.

## Scope

### In Scope

- `on="key"` and `on=["k1", "k2"]` using-key joins.
- Right and full join diagnostics hardening.
- Cross join safety through explicit Cartesian acknowledgement.
- Supported join strategy directives beyond broadcast.
- Forward as-of joins.
- Backend capability checks, diagnostics, explain, traceability, docs, compatibility tables, and parity tests.

### Out of Scope

- Automatic cost-based join reordering.
- Nearest as-of joins.
- Stream-stream joins.
- Raw SQL join predicates.
- Lateral joins and table-valued-function joins unless a later accepted design admits them.

## ExecPlan

`docs/dev/planning/done/P07072603.V3-join-pyspark-parity-hardening.plan.md`

## Engineering Tasks

1. Add using-key source capture and validation.
2. Harden nullable-side diagnostics for right and full joins.
3. Enforce Cartesian join acknowledgement.
4. Add strategy directives and capability diagnostics.
5. Add forward as-of semantics, tests, docs, and generated examples.

## Acceptance Criteria

- Using-key joins work for one key and multiple keys.
- Right/full diagnostics name nullable sides and invalid output fields.
- Cross joins cannot compile without explicit acknowledgement.
- Strategy directives render or fail through capability diagnostics.
- Forward as-of joins have online/generated parity tests.
- `make build` passes.

## Progress

- [x] Implement v3 join parity hardening (completed 2026-07-12).
