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
- Add stable diagnostics and generated-source boundary checks for Arrow UDF/UDTF and unsupported state APIs. Row-based
  `transformWithState` is admitted for ordinary PySpark 4.1 with independent ledger status and live mode/timer/restart
  evidence. Close the separate `transformWithStateInPandas` evidence gate for PySpark 4.0/4.1; retain the legacy
  `applyInPandasWithState` gate for PySpark 3.5/4.0/4.1; never combine their status with the row family.
- Record `foreach` and `foreachBatch` as caller-owned handoffs, including writer construction, callback retries,
  checkpointing, and idempotence; generated Structure code must not start either lifecycle.
- Add streaming classification and caller-owned examples where needed.
- At Sprint58, typed row state used one `ValueState` and callback-scoped timer values. The subsequent typed parity
  continuation implemented named Value/List/Map state, ProcessingTime TTL, and typed initial state; processor cleanup
  and automatic state-schema evolution remain outside the typed contract.

## Acceptance

Catalog status, diagnostics, specification, and tests agree; gated APIs are rejected with their documented remedy.

## Governing plan

`docs/dev/planning/P08042601.V11-pyspark-4.1-adoption.plan.md`,
`docs/dev/planning/P10042603.V11-transform-with-state-admission-and-typed-parity.plan.md`,
`docs/dev/planning/past/P10042604.V11-transform-with-state-in-pandas.plan.md`,
`docs/dev/planning/P10062602.V11-apply-in-pandas-with-state.plan.md`,
`docs/dev/planning/past/P10042602.Row-level-foreach-sinks.plan.md`,
`docs/dev/planning/past/P10052601.Schema-declared-foreach-batch-sinks.plan.md`, and the V11 Python/streaming and
observations designs.
