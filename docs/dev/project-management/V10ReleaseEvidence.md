# V10 Release Evidence and Deferred Follow-Up

Date: 2026-08-22

This report is the V10 evidence matrix and deferred-follow-up register. It separates implementation closure from
runtime support claims: a skipped or unavailable live lane is recorded as missing evidence, never as a pass.

## Release decision

V10 is not yet cleared for an unconditional runtime-support claim. The compiler, generated-code, online symbolic,
diagnostic, documentation, and package gates are green. The remaining release blocker is external live evidence:
the local Docker integration harness cannot access the Docker engine named pipe, and the SearchDocuments streaming
proving lane remains design-gated rather than supported.

## Evidence summary

| Area | Current disposition | Evidence | Remaining condition |
| --- | --- | --- | --- |
| API Catalog and schema evolution | Implemented batch slices; explicit gates retained for streaming missing-column union, XML, Variant mutation, and join reordering | `docs/APICatalog.md`; focused catalog, Geometry, sampling, and relation-union tests; `make build` | Run the pinned live schema-evolution lane before changing the streaming ledger |
| Geometry and sampling | Implemented provider-neutral/literal contracts; sampling is batch-only | `tests/specifications/v9-api-catalog/test_v9_geometry.py`; `test_v9_relation_sampling.py`; APICatalog rows | Optional-provider evidence remains target-gated and is not bundled |
| Streaming state metadata and joins | Existing admitted shapes supported; cross/anti, global selected-row, broad analytic windows, and chained arbitrary state remain rejected or design-gated | `tests/specifications/streaming-compatibility/test_v1_streaming_compatibility.py`; streaming coverage ledger; explain/state-stage tests | Pinned PySpark 3.5/4.0 parity and restart evidence for any promoted shape |
| Caller-owned side effects | `foreachBatch` is caller-owned-guided with restart/retry evidence; row `foreach` and arbitrary state remain design-gated | `examples/streams/adoption.py`; `tests/integration/pyspark/v10/test_foreach_batch_restart.py`; streaming ledger | No Structure-owned lifecycle or sink runtime is admitted |
| Typed scalar/map generators and Search chunking | Implemented with generated/online/compiler/traceability coverage; live backend lane unavailable locally | `P08082601.Typed-scalar-generators-and-optimizer-visible-search-chunking.plan.md`; V6/V7 focused suites; generated artifacts | Run live ordinary-PySpark and Spark Connect conformance when Docker access is restored |
| Ordinal-aware higher-order callbacks | Implemented and reconciled; unary/binary callback forms preserve typed zero-based indexes | `P08082602.Ordinal-aware-higher-order-array-callbacks.plan.md`; higher-order diagnostics/rendering tests | No remaining environment-independent implementation gap |
| Search vector index/RRF and inference | Architecture and typed exact implementation complete; live vector retrieval evidence unavailable | `P08052602.Search-vector-index-and-rrf.plan.md`; Search vector/index/vectorization tests; `make build` | Run live exact retrieval and validation-failure evidence |
| Collision-safe generated identities | Implemented and covered by no-Spark uniqueness/file-map tests and build | `P08042601.Collision-safe-generated-identities.plan.md`; generated-owner uniqueness tests | Re-run the integration identity lane after Docker access is restored |

## Validation run

The workspace-local build completed on 2026-08-21:

- `make build`: 1,647 passed, 66 skipped.
- Secondary rigidity/compatibility gate: 73 passed, 6 skipped.
- Package sdist and wheel built successfully.
- The default Windows pytest temp directory was inaccessible; redirecting `TEMP`/`TMP` to a workspace-local directory
  made all four affected fixture tests pass (`12 passed`).

The live-lane attempt on 2026-08-22 was:

```text
poetry run python scripts/run_integration.py --backend pyspark35
```

It stopped before test execution because Docker reported permission denied while connecting to
`npipe:////./pipe/docker_engine`. Docker is installed, but this environment cannot currently provide live Spark
evidence.

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
| Restore Docker engine access and run ordinary PySpark 3.5/4.0 lanes | Development environment | `make integration BACKEND=pyspark35`, then `pyspark40` |
| Run Spark Connect 3.5/4.0 parity lanes | Development environment | `make integration BACKEND=spark-connect35`, then `spark-connect40` |
| Run exact vector retrieval live evidence | Search proving lane | Focused Search integration test plus generated/online output comparison |
| Resume SearchDocuments streaming design | Structure/Search design owner | Bounded-state design, generated report, live restart fixture, and caller handoff recipe |
| Optional Geometry provider evidence | Optional-provider integration owner | Pinned provider environment and separate optional-provider test lane |

Until those lanes produce positive evidence, the corresponding rows must retain their current gated, caller-owned,
streaming-ineligible, or unavailable status.
