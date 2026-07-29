# Sprint 37: V8 Stateless Streaming Gaps

Status: complete. The stateless slice admits typed struct generators and exact-schema stream-stream `union_all(...)` /
`union_by_name(...)`; ordering and priority-selection are closed as explicit streaming-ineligible rows. Effective
checked parity is 32 / 32, or 100.0 percent. Focused local evidence:
`PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py tests/specifications/compatibility/test_pyspark_structured_streaming_coverage.py tests/specifications/compatibility/test_pyspark_transformation_coverage.py tests/specifications/v6-api-ledger/test_v6_relation_union.py tests/specifications/v6-api-ledger/test_v6_posexplode_struct.py tests/specifications/v7-api-ledger/test_v7_explode_struct.py tests/integration/pyspark/v8/test_stateless_streaming_gaps.py --maxfail=5 -rs`
passed with 102 tests and 3 skipped integration tests on 2026-07-29.

## Sprint Goal

Admit the safe stateless portions of the current batch-only streaming gaps.

## User-Facing Outcome

Typed row-local expansion and union-like transformations that Spark accepts on streaming DataFrames work in online and
generated Structure execution without Structure owning query lifecycle.

## In Scope

- Design and implementation for streaming-safe typed struct generators if live PySpark accepts them. Complete locally;
  live PySpark lane remains the external evidence gate.
- Operation-level set-family split, with union-like operations admitted only when restart evidence passes. First slice
  admits stream-stream exact-schema `union_all(...)` and `union_by_name(...)`.
- Static diagnostics for set operations that Spark rejects on streaming DataFrames. First slice keeps `intersect(...)`,
  `intersect_all(...)`, `subtract(...)`, and `except_all(...)` streaming-ineligible.
- Explain, traceability, generated-source scans, and PySpark 3.5/4.0 file-stream restart evidence.

## Out of Scope

- Distinct, intersect, subtract, and except streaming claims without explicit Spark evidence.
- Ordering, limits, selected-row priority selection, lifecycle ownership, and Spark Connect streaming.

## Acceptance

Each admitted stateless operation returns a streaming DataFrame in online and generated execution, survives restart from
a caller-owned checkpoint, and keeps generated source free of lifecycle and action APIs.

Acceptance is complete through static checks, generated-source scans, and targeted live PySpark 3.5/4.0 restart
evidence in `tests/integration/pyspark/v8/test_stateless_streaming_gaps.py`.

## Governing Documents

`docs/dev/specifications/V8StructuredStreamingCoverageParity.md` and
`docs/dev/planning/done/P07292601.V8-pyspark-structured-streaming-coverage-parity.plan.md`
