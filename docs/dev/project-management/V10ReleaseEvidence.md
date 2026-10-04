# V10 Release Evidence and Deferred Follow-Up

Date: 2026-10-04

This report is the V10 evidence matrix and deferred-follow-up register. It separates implementation closure from
runtime support claims: a skipped or unavailable live lane is recorded as missing evidence, never as a pass.

## Focused Search and V10 closeout follow-up: 2026-10-04

Focused Search proving tests pass on ordinary PySpark 3.5/4.0 and Spark Connect 3.5/4.0, without driver heap exhaustion
in the final focused runs. The 2026-10-04 Connect 3.5 selection completed with 22 passed and 4 skipped (including the
live concept selection); the full classic 3.5 selection also passed every Search test. Bounded, run-scoped singleton-policy checks and earlier checkpoints inside
`FuseDocuments` preserve online/generated results and candidate validation. Additional live tests cover zero/multiple
policy rows, nullable values, actual schema parity, duplicate candidate keys, and streaming-frame bypass. Connect
assertion failures also retain their original diagnostic instead of being mislabeled from server stack-trace text.

The isolated, profiled 3.5 reranking comparison reduced combined construction from 140.63s to 56.79s; it is one
comparison, not a median or a universal speedup claim. See [Search performance evidence](../issues/I09272601/SearchIntegrationPerformance.bench.md)
for exact commands, intermediate failures, corrected safety tests, timings, and storage tradeoffs. This supersedes
the Search-specific failure observations in the historical full-lane matrix below, not the entire V10 release
decision. Connect 3.5 was subsequently revalidated: the current `test_search.py` selection completed with 22 passed
and 4 skipped, including exact-vector validation cases. The 12-case vector validation selection passed on each of the
four backends. Search streaming remains design-gated.

## Release decision

V10's implementation and named dispositions are reconciled against the completed SQL-baseline, temporal, JSON-tuple,
typed-SQL, and geospatial-boundary plans. The refreshed PySpark 3.5 full selection is 231 passed, 7 skipped, and 6
failed; Search and V10 streaming checks pass, while failures remain in V1/V2 order-hook schema contracts and V11
generated-module import paths. Exact vector validation passes on all four backends. The evidence distinction is that
the broad full selection was refreshed on classic 3.5, while focused Search and vector slices were refreshed on Connect
3.5 and prior focused Search runs cover classic 4.0 and Connect 4.0. SearchDocuments streaming and other explicit gates
remain unsupported.

The live baseline is the shared worktree on 2026-10-04. Full selections were attempted on all four backend lanes, but
only classic PySpark 3.5 completed. Focused Search and vector results are recorded separately and do not stand in for
full-lane counts.

## Evidence summary

| Area | Current disposition | Evidence | Remaining condition |
| --- | --- | --- | --- |
| API Catalog and schema evolution | Implemented batch slices; explicit gates retained for streaming missing-column union, XML, Variant mutation, and join reordering | `docs/API.md`; completed `P09302601` runtime matrix; missing-column streaming remains batch-only | No streaming-ledger promotion without a separately admitted contract |
| Geometry and sampling | Implemented provider-neutral/literal contracts; sampling is batch-only | `tests/specifications/v9-api-catalog/test_v9_geometry.py`; `test_v9_relation_sampling.py`; APICatalog rows | Optional-provider evidence remains target-gated and is not bundled |
| Streaming state metadata and joins | Existing admitted shapes supported; cross/anti, global selected-row, broad analytic windows, and chained arbitrary state remain rejected or design-gated | Classic 3.5/4.0 file-stream, restart, and `foreachBatch` checks; see streaming plan | No new state shape is promoted; SearchDocuments stays design-gated |
| Caller-owned side effects | `foreachBatch` is caller-owned-guided with restart/retry evidence on ordinary PySpark 3.5 and 4.0; row `foreach` and arbitrary state remain design-gated | `examples/streams/adoption.py`; `tests/integration/pyspark/v10/test_foreach_batch_restart.py`; streaming ledger | No Structure-owned lifecycle or sink runtime is admitted |
| Typed scalar/map generators and Search chunking | Implemented with generated/online/compiler/traceability coverage; current bounded Search cases pass | `P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md`; refreshed classic 3.5 selection | No Search plan-scale failure appeared in the refreshed classic 3.5 run |
| Ordinal-aware higher-order callbacks | Implemented and reconciled; unary/binary callback forms preserve typed zero-based indexes | `P08082602.Ordinal-aware-higher-order-array-callbacks.plan.md`; higher-order diagnostics/rendering tests | No remaining environment-independent implementation gap |
| Search vector index/RRF and inference | Exact vector retrieval/validation has live evidence; hosted inference and ANN remain outside scope | `P08052602.Search-vector-index-and-rrf.plan.md`; 12 vector validation cases passed on all four backends | No remaining V10 vector-index proving task identified; preserve provider boundary |
| Collision-safe generated identities | Implemented and covered by no-Spark uniqueness/file-map tests and build | `P08042601.Collision-safe-generated-identities.plan.md`; generated-owner uniqueness tests | Re-run the integration identity lane when promoting the live backend matrix |

## Validation run

The final workspace-local build completed on 2026-10-04:

- `make build`: 2,128 passed, 245 skipped.
- Secondary golden/differential/metamorphic/compatibility gate: 109 passed, 7 skipped.
- Formatting, flake8, mypy, package sdist, and wheel all passed.

The first live-lane attempt on 2026-08-22 was:

```text
poetry run python scripts/run_integration.py --backend pyspark35
```

It stopped before test execution because Docker reported permission denied while connecting to
`npipe:////./pipe/docker_engine`.

The 2026-10-04 refreshes used the Compose definitions under `infra/compose/` and ran the current 244-test selection.
Focused evidence below supplements full-lane outcomes; an incomplete run is never recorded as a pass.

| Lane | Result | Live scope |
| --- | --- | --- |
| `pyspark35` | 231 passed, 7 skipped, 6 failed | Current full selection (244 collected). Search, vector, and V10 streaming tests pass. Failures are one V1 and one V2 order-hook schema contract plus four V11 generated-import tests. |
| `pyspark40` | Incomplete; 244 collected, timed out at 3,600s | Reached 74%, during `v2/advanced_order_analytics`; Search and V10 restart checks had passed. Visible failures were V1 order-hook schema and four V11 generated-import cases. No final summary. |
| `spark-connect35` | Incomplete; 244 collected, operator-interrupted after over one hour | Multiple failures arose during the long book-contract section before Search was reached; no final summary. Focused Search: 22 passed/4 skipped; vector validation: 12 passed. |
| `spark-connect40` | Incomplete; 244 collected, stopped after about 27m without progress | No final summary. Focused Search module and vector validation (12 passed) are separately verified. |

The 2026-10-04 classic 3.5 full selection no longer reproduces the prior Search heap failures. The remaining six failures
are outside V10: the V1/V2 generated order hooks return relations missing the physical `promo-code` field, and four
V11 scalar assertion tests fail generated-module import-path checks. The Connect 3.5 focused Search selection included
26 tests across the specified module plus live concept paths, yielding 22 passed and 4 skipped; the skips are the
expected concept/runtime exclusions. It does not constitute a broad Connect 3.5 full-lane result.

Classic PySpark 3.5 and 4.0 passed the V10 `foreachBatch` restart/retry tests (6 passed, 3 skipped on 3.5; 9 passed
on 4.0), plus the admitted file-stream examples. No new stateful shape is promoted. Spark Connect correctly skips
classic-only restart/stateful tests.

Exact vector validation now has live negative evidence on all four runtimes: dimension, model ID, content revision,
experiment ID, empty vectors, and zero-norm vectors, each through online and generated execution (12 passed per
backend). The focused Search proving paths also pass on classic 3.5/4.0 and Connect 3.5/4.0 across the dated focused
runs. These bounded results do not claim broad full-Search streaming support.

On 2026-08-23, a self-sufficient PySpark-only reproducer was added at
`docs/troubleshooting/memory/spark_driver_heap_oom.py`. With two
input rows and a 1 GiB driver, seven rounds of reused self-join/reverse/union lineage grew the unresolved logical-plan
text from 1,211 to 16,680,913 characters and failed during `count()` with `java.lang.OutOfMemoryError: Java heap space`.
Six rounds completed. Adding `localCheckpoint()` after each round kept the plan at 46-48 characters through eight rounds and
completed successfully, confirming lineage duplication—not input cardinality or executor memory—as the primary mechanism.
The detailed RCA, measurements, and decisions are recorded in `docs/dev/issues/I09272602/Memory-evidence.md`; end-user
commands are in `docs/troubleshooting/memory/spark_driver_heap_oom.gotcha.md`.

Completed post-baseline plans provide additional focused PySpark 3.5/4.0 and Connect 3.5/4.0 SQL and temporal evidence;
see [P09302601](../planning/past/P09302601.PySpark-SQL-baseline-gap-closeout.plan.md) and
[P10022601](../planning/past/P10022601.PySpark-temporal-baseline-closeout.plan.md). The final `make build` on 2026-10-04 passed: main suite 2,128
passed/245 skipped; secondary suite 109 passed/7 skipped; formatting, flake8, mypy, sdist, and wheel succeeded.

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
| Broad Connect full-suite evidence, if required for a suite-level promotion | Release owner / Connect runtime | Both 2026-10-04 broad runs failed to produce summaries; focused family evidence passes and remains the supported claim boundary. |
| Recheck whole-document cross-links after plan archival | Release closeout | Validate archived-plan links and `git diff --check`; no implementation work remains in the archived plans. |
| Resume SearchDocuments streaming design | Separate Search design owner; not a V10 release blocker while explicitly gated | Bounded-state design, generated report, live restart fixture, and caller handoff recipe |
| Broaden optional Geometry provider evidence | Optional-provider integration owner | Pinned Sedona WKT round-trip passes in all four selected lanes; add separate provider tests for CRS, measurements, joins, indexes, and collections |

Until those lanes produce positive evidence, the corresponding rows must retain their current gated, caller-owned,
streaming-ineligible, or unavailable status.
