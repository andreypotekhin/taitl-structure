# V10 Release Evidence and Deferred Follow-Up

Date: 2026-09-25

This report is the V10 evidence matrix and deferred-follow-up register. It separates implementation closure from
runtime support claims: a skipped or unavailable live lane is recorded as missing evidence, never as a pass.

## Focused Search follow-up: 2026-09-27

All seven tests in `test_search.py` now pass on ordinary PySpark 3.5, ordinary 4.0, and Spark Connect 4.0, without
driver heap exhaustion in the final runs. Bounded, run-scoped singleton-policy checks and earlier checkpoints inside
`FuseDocuments` preserve online/generated results and candidate validation. Additional live tests cover zero/multiple
policy rows, nullable values, actual schema parity, duplicate candidate keys, and streaming-frame bypass. Connect
assertion failures also retain their original diagnostic instead of being mislabeled from server stack-trace text.

The isolated, profiled 3.5 reranking comparison reduced combined construction from 140.63s to 56.79s; it is one
comparison, not a median or a universal speedup claim. See [Search performance evidence](../issues/I09272601/SearchIntegrationPerformance.bench.md)
for exact commands, intermediate failures, corrected safety tests, timings, and storage tradeoffs. This supersedes
the Search-specific failure observations in the historical full-lane matrix below, not the entire V10 release
decision. Connect 3.5 and unrelated full-lane failures were not revalidated. Search streaming remains design-gated.

## Release decision

V10 is conditionally closed, not cleared for an unconditional runtime-support claim. The compiler, generated-code,
online symbolic, diagnostic, documentation, and package gates are green for the current closeout baseline. Docker is
available. The current PySpark 3.5 rerun shows that the generated stage-result contract failure is fixed: Search and
Security now reach their runtime assertions. Two Search cases still fail while Spark constructs the large logical plan
with `java.lang.OutOfMemoryError: Java heap space`; exact vector retrieval therefore remains unproven. The
SearchDocuments streaming proving lane remains design-gated rather than supported.

The current live baseline is the shared worktree at the time of the run. The 2026-09-25 full-lane results for PySpark
3.5, Spark Connect 3.5, and Spark Connect 4.0 are recorded below. The PySpark 4.0 full lane was started but did not
reach a final summary within the bounded run window; its bounded Security check passed separately. The controlled
plan-size and driver-memory experiments remain relevant because the current ordinary-PySpark Search failures reproduce
that same lineage-growth mechanism.

## Evidence summary

| Area | Current disposition | Evidence | Remaining condition |
| --- | --- | --- | --- |
| API Catalog and schema evolution | Implemented batch slices; explicit gates retained for streaming missing-column union, XML, Variant mutation, and join reordering | `docs/API.md`; focused catalog, Geometry, sampling, and relation-union tests; `make build` | Run the pinned live schema-evolution lane before changing the streaming ledger |
| Geometry and sampling | Implemented provider-neutral/literal contracts; sampling is batch-only | `tests/specifications/v9-api-catalog/test_v9_geometry.py`; `test_v9_relation_sampling.py`; APICatalog rows | Optional-provider evidence remains target-gated and is not bundled |
| Streaming state metadata and joins | Existing admitted shapes supported; cross/anti, global selected-row, broad analytic windows, and chained arbitrary state remain rejected or design-gated | `tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py`; streaming coverage ledger; explain/state-stage tests | Pinned PySpark 3.5/4.0 parity and restart evidence for any promoted shape |
| Caller-owned side effects | `foreachBatch` is caller-owned-guided with restart/retry evidence on ordinary PySpark 3.5 and 4.0; row `foreach` and arbitrary state remain design-gated | `examples/streams/adoption.py`; `tests/integration/pyspark/v10/test_foreach_batch_restart.py`; streaming ledger | No Structure-owned lifecycle or sink runtime is admitted |
| Typed scalar/map generators and Search chunking | Implemented with generated/online/compiler/traceability coverage; ordinary PySpark 3.5/4.0 lanes execute, while Search scale and Connect evidence remain open | `P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md`; V6/V7 focused suites; generated artifacts | Resolve the Search plan-scale gate and obtain bounded Spark Connect evidence |
| Ordinal-aware higher-order callbacks | Implemented and reconciled; unary/binary callback forms preserve typed zero-based indexes | `P08082602.Ordinal-aware-higher-order-array-callbacks.plan.md`; higher-order diagnostics/rendering tests | No remaining environment-independent implementation gap |
| Search vector index/RRF and inference | Architecture and typed exact implementation complete; live vector retrieval evidence remains unproven | `P08052602.Search-vector-index-and-rrf.plan.md`; Search vector/index/vectorization tests; `make build` | Run live exact retrieval and validation-failure evidence |
| Collision-safe generated identities | Implemented and covered by no-Spark uniqueness/file-map tests and build | `P08042601.Collision-safe-generated-identities.plan.md`; generated-owner uniqueness tests | Re-run the integration identity lane when promoting the live backend matrix |

## Validation run

The final workspace-local build completed on 2026-09-25:

- `make build`: 1,856 passed, 179 skipped.
- Secondary rigidity/compatibility gate: 73 passed, 7 skipped.
- Package sdist and wheel built successfully.

The first live-lane attempt on 2026-08-22 was:

```text
poetry run python scripts/run_integration.py --backend pyspark35
```

It stopped before test execution because Docker reported permission denied while connecting to
`npipe:////./pipe/docker_engine`.

The current Docker rerun on 2026-09-25 used the Compose definitions under `infra/compose/` and ran the full 178-test
selection for each lane:

| Lane | Result | Live scope |
| --- | --- | --- |
| `pyspark35` | 164 passed, 6 skipped, 8 failed | Current full integration and live concept selection; generated stage-result failures no longer occur. Two Search cases hit Java heap exhaustion; six unrelated live-contract/import failures remain. |
| `pyspark40` | Incomplete; no final count | Current full lane did not reach a final summary after approximately one hour and was interrupted while processing the heavy Search/Spark workload. A bounded Security test passed (`1 passed, 177 deselected`). The old 56/3/6 result remains a historical focused checkpoint only. |
| `spark-connect35` | 148 passed, 19 skipped, 11 failed | Current full integration and live concept selection. Five Search cases fail during rendering, one v1 order contract fails, four v11 generated-import tests fail, and one v2 order contract fails. |
| `spark-connect40` | 151 passed, 16 skipped, 11 failed | Current full integration and live concept selection. Five Search cases fail during rendering with an unsupported `array` helper call, one v1 order contract fails, four v11 generated-import tests fail, and one v2 order contract fails. |

The current ordinary-PySpark 3.5 failures are no longer the historical generated-result contract failure. The two
V10-relevant Search failures occur in `test_text_fixture_runs_online_and_generated` and
`test_document_search_reranks_bm25_candidates_for_multiple_queries`; both exhaust the Spark driver heap while building
repeated union/reverse logical plans. The ordinary 4.0 lane did not reach a final summary, but its bounded Security
check passed. On Connect 3.5 and 4.0, the five Search failures occur during generated rendering; Connect 4.0 exposes
the concrete unsupported helper call as `array`. The remaining current-lane failures are the v1/v2 order hook schema
contract and v11 scalar-assertion generated-module import-path tests; they are tracked outside the V10 stage-result
scope.

The ordinary lanes also passed `tests/integration/pyspark/v10/test_foreach_batch_restart.py`, the v7 stream/static
restart tests, the v8 stateless streaming gate tests, and the v9 Sedona geometry test. The focused Connect lanes passed
the Connect boundary/UDF tests, v7 binary/collection/deterministic/schema/struct tests, v9 geometry, and the selected
V3 concept parity tests; Connect correctly skipped classic-PySpark-only restart and stateful streaming tests.

Exact vector retrieval and the Search generated/online comparison remain unproven because the Search proving cases fail
during logical-plan construction before the generated result can be compared. The full Connect Search proving lane was
not claimed from the focused run.

On 2026-08-23, a self-sufficient PySpark-only reproducer was added at
`docs/troubleshooting/memory/spark_driver_heap_oom.py`. With two
input rows and a 1 GiB driver, seven rounds of reused self-join/reverse/union lineage grew the unresolved logical-plan
text from 1,211 to 16,680,913 characters and failed during `count()` with `java.lang.OutOfMemoryError: Java heap space`.
Six rounds completed. Adding `localCheckpoint()` after each round kept the plan at 46-48 characters through eight rounds and
completed successfully, confirming lineage duplication—not input cardinality or executor memory—as the primary mechanism.
The detailed RCA, measurements, and decisions are recorded in `docs/dev/issues/I09272602/Memory-evidence.md`; end-user
commands are in `docs/troubleshooting/memory/spark_driver_heap_oom.gotcha.md`.

The current Connect runs do not provide positive full-Search evidence: both Connect lanes fail all five selected Search
proving cases during generated rendering. They do provide broad positive evidence for the remaining selected boundary,
UDF, generator, parsing, geometry, and concept-parity cases. Connect also correctly skips classic-PySpark-only restart
and stateful streaming tests. This does not clear the full Search or streaming-state gates.

The workspace-local build passed after the generated stage-result compatibility regression test was added. The current
Docker rerun additionally confirms that the remaining Search blocker is the previously documented logical-plan memory
growth, not the generated result boundary. Historical plan-size and driver-memory experiments therefore remain direct
evidence for the current Search blocker and are not used to convert the failing Search cases into positive evidence.

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
| Reduce Search logical-plan growth, then re-run ordinary PySpark 3.5/4.0 Search evidence | Search implementation owner | Apply the checkpoint/materialization decision from `docs/dev/issues/I09272602/Memory-evidence.md`, then run the exact Search tests and both ordinary lanes |
| Obtain bounded Spark Connect 3.5/4.0 Search parity evidence | Development environment | Focused non-Search Connect slices pass; reduce the Search proving fixture or provide a larger Connect driver, then rerun `make integration BACKEND=spark-connect35` and `spark-connect40` |
| Run exact vector retrieval live evidence | Search proving lane | Focused Search integration test plus generated/online output comparison |
| Resume SearchDocuments streaming design | Structure/Search design owner | Bounded-state design, generated report, live restart fixture, and caller handoff recipe |
| Broaden optional Geometry provider evidence | Optional-provider integration owner | Pinned Sedona WKT round-trip passes in all four selected lanes; add separate provider tests for CRS, measurements, joins, indexes, and collections |

Until those lanes produce positive evidence, the corresponding rows must retain their current gated, caller-owned,
streaming-ineligible, or unavailable status.
