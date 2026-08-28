# V10 Release Evidence and Deferred Follow-Up

Date: 2026-08-27

This report is the V10 evidence matrix and deferred-follow-up register. It separates implementation closure from
runtime support claims: a skipped or unavailable live lane is recorded as missing evidence, never as a pass.

## Release decision

V10 is conditionally closed, not cleared for an unconditional runtime-support claim. The compiler, generated-code,
online symbolic, diagnostic, documentation, and package gates are green for the current closeout baseline. Docker is now
available and has produced live evidence for the ordinary PySpark 3.5/4.0 lanes plus focused Spark Connect 3.5/4.0
boundary and parity slices. The full ordinary lanes still expose six shared generated-result failures (four Search cases,
the generated security fixture, and the chained event-time window); the SearchDocuments streaming proving lane remains
design-gated rather than supported.

The current live baseline is the shared worktree at the time of the run. The generated-result failures are recorded as
implementation evidence, not converted into support claims. Earlier controlled plan-size and driver-memory experiments
remain historical evidence below; they do not override the current failing generated contract.

## Evidence summary

| Area | Current disposition | Evidence | Remaining condition |
| --- | --- | --- | --- |
| API Catalog and schema evolution | Implemented batch slices; explicit gates retained for streaming missing-column union, XML, Variant mutation, and join reordering | `docs/APICatalog.md`; focused catalog, Geometry, sampling, and relation-union tests; `make build` | Run the pinned live schema-evolution lane before changing the streaming ledger |
| Geometry and sampling | Implemented provider-neutral/literal contracts; sampling is batch-only | `tests/specifications/v9-api-catalog/test_v9_geometry.py`; `test_v9_relation_sampling.py`; APICatalog rows | Optional-provider evidence remains target-gated and is not bundled |
| Streaming state metadata and joins | Existing admitted shapes supported; cross/anti, global selected-row, broad analytic windows, and chained arbitrary state remain rejected or design-gated | `tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py`; streaming coverage ledger; explain/state-stage tests | Pinned PySpark 3.5/4.0 parity and restart evidence for any promoted shape |
| Caller-owned side effects | `foreachBatch` is caller-owned-guided with restart/retry evidence on ordinary PySpark 3.5 and 4.0; row `foreach` and arbitrary state remain design-gated | `examples/streams/adoption.py`; `tests/integration/pyspark/v10/test_foreach_batch_restart.py`; streaming ledger | No Structure-owned lifecycle or sink runtime is admitted |
| Typed scalar/map generators and Search chunking | Implemented with generated/online/compiler/traceability coverage; ordinary PySpark 3.5/4.0 lanes execute, while Search scale and Connect evidence remain open | `P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md`; V6/V7 focused suites; generated artifacts | Resolve the Search plan-scale gate and obtain bounded Spark Connect evidence |
| Ordinal-aware higher-order callbacks | Implemented and reconciled; unary/binary callback forms preserve typed zero-based indexes | `P08082602.Ordinal-aware-higher-order-array-callbacks.plan.md`; higher-order diagnostics/rendering tests | No remaining environment-independent implementation gap |
| Search vector index/RRF and inference | Architecture and typed exact implementation complete; live vector retrieval evidence remains unproven | `P08052602.Search-vector-index-and-rrf.plan.md`; Search vector/index/vectorization tests; `make build` | Run live exact retrieval and validation-failure evidence |
| Collision-safe generated identities | Implemented and covered by no-Spark uniqueness/file-map tests and build | `P08042601.Collision-safe-generated-identities.plan.md`; generated-owner uniqueness tests | Re-run the integration identity lane when promoting the live backend matrix |

## Validation run

The final workspace-local build completed on 2026-08-27:

- `make build`: 1,714 passed, 66 skipped.
- Secondary rigidity/compatibility gate: 73 passed, 6 skipped.
- Package sdist and wheel built successfully.
- The default Windows pytest temp directory was inaccessible; redirecting `TEMP`/`TMP` to a workspace-local directory
  made all four affected fixture tests pass (`12 passed`).

The first live-lane attempt on 2026-08-22 was:

```text
poetry run python scripts/run_integration.py --backend pyspark35
```

It stopped before test execution because Docker reported permission denied while connecting to
`npipe:////./pipe/docker_engine`.

The Docker retry on 2026-08-27 used the Compose definitions under `infra/compose/`. It rebuilt the pinned PySpark 3.5
image and ran the full 65-test selection on both ordinary targets:

| Lane | Result | Live scope |
| --- | --- | --- |
| `pyspark35` | 53 passed, 6 skipped, 6 failed | Full integration and live concept selection; foreachBatch restart and Sedona geometry passed. |
| `pyspark40` | 56 passed, 3 skipped, 6 failed | Full integration and live concept selection; foreachBatch restart and Sedona geometry passed. |
| `spark-connect35` | 15 passed, 9 skipped | Focused Connect boundary, UDF, generator, parsing, geometry, and concept parity slice; Search was excluded. |
| `spark-connect40` | 18 passed, 6 skipped | Same focused Connect slice; Search was excluded. |

The six ordinary failures are the same shared generated-result contract failure in four Search cases, the generated
security fixture, and the generated chained event-time window. The failing path raises
`TypeError: Generated transform executor must return a stage-aware TransformResult when composed stage outputs are
enabled`. This is positive evidence that the Docker/runtime lane is executing the current code, but it is not positive
feature evidence for those failing cases.

The ordinary lanes also passed `tests/integration/pyspark/v10/test_foreach_batch_restart.py`, the v7 stream/static
restart tests, the v8 stateless streaming gate tests, and the v9 Sedona geometry test. The focused Connect lanes passed
the Connect boundary/UDF tests, v7 binary/collection/deterministic/schema/struct tests, v9 geometry, and the selected
V3 concept parity tests; Connect correctly skipped classic-PySpark-only restart and stateful streaming tests.

Exact vector retrieval and the Search generated/online comparison remain unproven because the Search proving cases fail
before the generated result can be compared. The full Connect Search proving lane was not claimed from the focused run.

On 2026-08-23, a self-sufficient PySpark-only reproducer was added at
`docs/troubleshooting/memory/spark_driver_heap_oom.py`. With two
input rows and a 1 GiB driver, seven rounds of reused self-join/reverse/union lineage grew the unresolved logical-plan
text from 1,211 to 16,680,913 characters and failed during `count()` with `java.lang.OutOfMemoryError: Java heap space`.
Six rounds completed. Adding `localCheckpoint()` after each round kept the plan at 46-48 characters through eight rounds and
completed successfully, confirming lineage duplication—not input cardinality or executor memory—as the primary mechanism.
The detailed RCA, measurements, and decisions are recorded in `docs/dev/specifications/Memory.spec.md`; end-user
commands are in `docs/troubleshooting/memory/spark_driver_heap_oom.gotcha.md`.

An earlier bounded Connect attempt did not progress through the full Search proving suite, so it remains unavailable as
full Search evidence. The focused 2026-08-27 Connect runs now provide positive evidence for the selected boundary, UDF,
generator, parsing, geometry, and concept-parity cases. This does not clear the full Search or streaming-state gates.

The workspace-local build used a writable pytest temp root and passed after the generated golden outputs were reconciled.
The current Docker retry additionally records the shared generated-result contract failure described above. Historical
plan-size and driver-memory experiments remain in the memory reproducer and are not used to convert the current failing
Search cases into positive evidence.

## SearchDocuments readiness matrix

| Contract | Current result | Required before promotion |
| --- | --- | --- |
| Event-time field and watermark | Candidate admission records `requested_at` and ten-minute watermarks | Preserve the same fields through the complete proving query |
| Bounded candidate and overlap state | Not proven; current graph contains global ranking and ordinary deduplication | Named finite top-K state with retention and deterministic ties |
| Final ranking state | Not proven; `row_number`/selected-row stages remain batch-only | Append-final result with no post-emission revisions |
| Stream/stream joins | Not proven end to end | Finite event-time bounds and compatible watermarks on both sides |
| Snapshot immutability | Design requirement only | One immutable index/score/feedback/policy snapshot per run |
| Restart recovery | Not proven for SearchDocuments | Isolated checkpoint restart with duplicate/late-event assertions |
| Caller-owned lifecycle handoff | Deferred | Document and execute the caller-owned source, sink, checkpoint, output-mode, and recovery handoff |

Therefore SearchDocuments remains batch-focused and design-gated for the streaming proving slice. Its current batch
path is not a V10 streaming support claim.

## Deferred owners and next commands

| Follow-up | Owner boundary | Acceptance command/evidence |
| --- | --- | --- |
| Re-run ordinary PySpark 3.5/4.0 Search evidence after a plan-level fix | Development environment | Run the exact tests from `docs/Gotchas.md`, then `make integration BACKEND=pyspark35` and `make integration BACKEND=pyspark40` |
| Obtain bounded Spark Connect 3.5/4.0 Search parity evidence | Development environment | Focused non-Search Connect slices pass; reduce the Search proving fixture or provide a larger Connect driver, then rerun `make integration BACKEND=spark-connect35` and `spark-connect40` |
| Run exact vector retrieval live evidence | Search proving lane | Focused Search integration test plus generated/online output comparison |
| Resume SearchDocuments streaming design | Structure/Search design owner | Bounded-state design, generated report, live restart fixture, and caller handoff recipe |
| Broaden optional Geometry provider evidence | Optional-provider integration owner | Pinned Sedona WKT round-trip passes in all four selected lanes; add separate provider tests for CRS, measurements, joins, indexes, and collections |

Until those lanes produce positive evidence, the corresponding rows must retain their current gated, caller-owned,
streaming-ineligible, or unavailable status.
