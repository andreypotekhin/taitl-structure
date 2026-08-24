# V10 Release Evidence and Deferred Follow-Up

Date: 2026-08-23

This report is the V10 evidence matrix and deferred-follow-up register. It separates implementation closure from
runtime support claims: a skipped or unavailable live lane is recorded as missing evidence, never as a pass.

## Release decision

V10 is conditionally closed, not cleared for an unconditional runtime-support claim. The compiler, generated-code,
online symbolic, diagnostic, documentation, and package gates are green for the current closeout baseline. Docker was
available for the initial proving runs and later recovered for the controlled rollback reproduction. The reciprocal
reduction hardening was rolled back after the experiment; the PySpark 3.5 and 4.0 lanes exposed Search generated-contract
and plan-scale defects; the
generated-contract defects are fixed, while the remaining Search proving cases exhaust the runner JVM heap during
online plan analysis. Spark Connect 3.5/4.0 collected successfully but did not complete bounded Search proving runs.
The SearchDocuments streaming proving lane remains design-gated rather than supported.

The current baseline uses the original reciprocal reduction graph so the heap failure is reproducible. The ordinary
driver-memory override and measured 1 GiB/3 GiB experiments are recorded below; a plan-level Search fix remains required
before runtime evidence can be promoted.

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

The final workspace-local build completed on 2026-08-22:

- `make build`: 1,661 passed, 66 skipped.
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

The retry used the Compose definitions under `infra/compose/` with escalation and rebuilt the PySpark 3.5 image. The
Windows checkout had made `run-integration.sh` CRLF; `infra/compose/images/pyspark/Dockerfile` now normalizes that
launcher during image build. The resulting full PySpark 3.5 lane collected 65 tests and reported 55 passed, 6 skipped,
and 4 Search failures. The failure repair lane then established 2 passed search parity cases and isolated the remaining
SearchDocuments case to a JVM `OutOfMemoryError: Java heap space` while Spark analyzed the online plan. The same live
run also passed `tests/integration/pyspark/v10/test_foreach_batch_restart.py`.

The rebuilt PySpark 4.0 lane collected 65 tests and reported 60 passed, 3 skipped, and 2 Search failures. Its
non-Search coverage and `foreach_batch_restart` case passed. The two Search failures are the text fixture and document
reranking cases; both exhaust the JVM while building or analyzing the large online plan. A focused rerun after removing
the priority-selection guard cross-join still reached the same class of heap exhaustion in `ReduceSimilarityScores`, so
the remaining Search issue is retained as a design/performance gate rather than claimed as an implementation pass.

The reciprocal-reduction rewrite that attempted to reduce plan duplication was deliberately rolled back for controlled
reproduction. The restored baseline reproduces the SearchDocuments failure on PySpark 3.5: the reranking test fails after
608.56 seconds with `java.lang.OutOfMemoryError: Java heap space` at a generated `DataFrame.union`, with Catalyst
`DeduplicateRelations` in the stack. The text fixture also fails in the same proving lane. A new ordinary-driver override
was added for experiments; the exact SearchDocuments case launched with `-Xmx3g` but produced no pytest result after a
16-minute bounded run, so increased heap alone is not a sufficient mitigation on the current Compose host.

On 2026-08-23, a self-sufficient PySpark-only reproducer was added at
`docs/troubleshooting/memory/spark_driver_heap_oom.py`. With two
input rows and a 1 GiB driver, seven rounds of reused self-join/reverse/union lineage grew the unresolved logical-plan
text from 1,211 to 16,680,913 characters and failed during `count()` with `java.lang.OutOfMemoryError: Java heap space`.
Six rounds completed. Adding `localCheckpoint()` after each round kept the plan at 46-48 characters through eight rounds and
completed successfully, confirming lineage duplication—not input cardinality or executor memory—as the primary mechanism.
The detailed RCA, measurements, and decisions are recorded in `docs/dev/specifications/Memory.spec.md`; end-user
commands are in `docs/troubleshooting/memory/spark_driver_heap_oom.gotcha.md`.

The Spark Connect 3.5 and 4.0 lanes both started successfully and collected all 65 tests. Connect 3.5 spent more than
20 minutes in the first Search test without pytest progress while the runner approached its 3 GiB driver allocation;
Connect 4.0 likewise did not progress beyond the first test within the bounded proving window. Both runs were stopped
after the timeout window and provide no positive Connect parity evidence. The Connect lanes therefore remain explicitly
unavailable for V10 promotion, with the development-environment owner responsible for a bounded Search fixture or a
larger-driver proving run.

The Docker engine later recovered after starting `com.docker.service` and Docker Desktop processes. The Compose stack was
then used for the rollback reproduction and the bounded 3 GiB experiment; those results are now the authoritative live
evidence for the current baseline.

The final workspace-local build used a writable pytest temp root and passed after the generated golden outputs were
reconciled. The live retry additionally fixed target deduplication, vector fallback typing, rank typing, and generated
schema-catalog defects. The remaining online SearchDocuments heap exhaustion is retained as a live design/performance
gate, not counted as positive release evidence.

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
| Obtain bounded Spark Connect 3.5/4.0 parity evidence | Development environment | Reduce the Search proving fixture or provide a larger Connect driver, then rerun `make integration BACKEND=spark-connect35` and `spark-connect40` |
| Run exact vector retrieval live evidence | Search proving lane | Focused Search integration test plus generated/online output comparison |
| Resume SearchDocuments streaming design | Structure/Search design owner | Bounded-state design, generated report, live restart fixture, and caller handoff recipe |
| Optional Geometry provider evidence | Optional-provider integration owner | Pinned provider environment and separate optional-provider test lane |

Until those lanes produce positive evidence, the corresponding rows must retain their current gated, caller-owned,
streaming-ineligible, or unavailable status.
