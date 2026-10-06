# Sprint 58: V11 Observations, Sketches, Python, and State Gates

Status: planned; target: 2027-02-05.

## Sprint goal

Close the contract decisions for complex observations, approximate sketches, Arrow UDF/UDTFs, the separate row and
Pandas state processor families, legacy `applyInPandasWithState`, and caller-owned side-effect handoffs.

## User-facing outcome

Users receive an honest supported contract or an actionable caller-owned remedy for every reviewed 4.1 Python and metric
API; no gated state or worker-Python API is generated accidentally.

## Implementation tasks

- Specify and implement a typed metric channel or retain the observation gate.
- Specify sketch binary/merge/dependency semantics or retain the gate.
- Add stable diagnostics and generated-source boundary checks for Arrow UDF/UDTF and unsupported state APIs. Reconcile
  the row-based `transformWithState` admission plan for PySpark 4.1, `transformWithStateInPandas` for PySpark 4.0/4.1,
  and legacy `applyInPandasWithState` for PySpark 3.5/4.0/4.1; keep support claims gated on each family's target
  evidence rather than promoting a combined arbitrary-state row.
- Record `foreach` and `foreachBatch` as caller-owned handoffs, including writer construction, callback retries,
  checkpointing, and idempotence; generated Structure code must not start either lifecycle.
- Add streaming classification and caller-owned examples where needed.
- The typed row subset uses one `ValueState` and callback-scoped timer values; keep additional Spark state types, TTL, typed initial state, processor cleanup, and state-schema evolution explicitly scoped in the row design/spec before admitting them.

## Acceptance

Catalog status, diagnostics, specification, and tests agree; gated APIs are rejected with their documented remedy.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`,
`docs/dev/planning/P10042603.V11-transform-with-state-admission-and-typed-parity.plan.md`,
`docs/dev/planning/P10042604.V11-transform-with-state-in-pandas.plan.md`,
`docs/dev/planning/P10062602.V11-apply-in-pandas-with-state.plan.md`,
`docs/dev/planning/P10042602.Row-level-foreach-sinks.plan.md`,
`docs/dev/planning/past/P10052601.Schema-declared-foreach-batch-sinks.plan.md`, and the V11 Python/streaming and
observations designs.
