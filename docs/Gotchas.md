# Gotchas

This page records reproducible operational traps that are easy to mistake for Structure compiler or transform defects.
Each entry uses the same `When` / `Error` / `Cause` / `Fix` shape as the development troubleshooting guide.

The detailed standalone lineage reproducer and avoidance guidance are in [the memory gotcha](troubleshooting/memory/spark_driver_heap_oom.gotcha.md). The developer measurements and design record are in the [Memory specification](dev/specifications/Memory.spec.md).

### Problem (integration): Search proving plan exhausts the ordinary PySpark driver heap

When: Running the bundled Search integration fixture against ordinary PySpark 3.5 or 4.0, especially the text fixture
or document-reranking case.

Error: The test spends several minutes in online/generated Search execution and fails while Spark analyzes or serializes
the logical plan. The useful failure is a JVM `OutOfMemoryError: Java heap space`; shutdown can add noisy Netty/RPC errors
after the Spark JVM has already failed.

Cause: This proving path does not execute `ReduceSimilarityScores`. `SearchDocuments` composes many retrieval, scoring, and
reranking branches over lazy indexed relations. Reusing those expanded relations across steps leaves Catalyst with a large
driver-side logical plan; the failure is not evidence that a worker task ran out of shuffle memory.

Fix: Reproduce the baseline before changing the Search graph, then try a larger driver heap. The repository Compose runner
accepts `STRUCTURE_SPARK_DRIVER_MEMORY` for ordinary PySpark; it is translated to `PYSPARK_SUBMIT_ARGS` before PySpark
launches its JVM. Do not rely on setting `spark.driver.memory` after the Spark session has already been created.

The smallest exact reproducer is the checked-in test
`tests/integration/pyspark/search/test_search.py::test_text_fixture_runs_online_and_generated`:

```python
# The complete fixture construction is in test_search.py. The failure is
# reached by running both execution modes through the same SearchDocuments graph.
online = SearchDocuments(**inputs).run(session(spark, execution_mode="online")).results
generated = SearchDocuments(**inputs).run(
    session(spark, execution_mode="generated", generated_package=PACKAGE)
).results
```

Run it from the repository root after Docker is ready:

```text
docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml up -d spark35-master spark35-worker
docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm -e INTEGRATION_PYTEST_ARGS="/workspace/tests/integration/pyspark/search/test_search.py -k test_text_fixture_runs_online_and_generated -q" structure-integration-pyspark35
```

The same reproduction with a larger driver heap is:

```text
docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml run --rm -e STRUCTURE_SPARK_DRIVER_MEMORY=3g -e INTEGRATION_PYTEST_ARGS="/workspace/tests/integration/pyspark/search/test_search.py -k test_text_fixture_runs_online_and_generated -q" structure-integration-pyspark35
```

Also try `test_document_search_reranks_bm25_candidates_for_multiple_queries` when validating the document-reranking
path. Record the PySpark version, driver-memory setting, elapsed time, exit status, and the first `OutOfMemoryError`
line. A passing larger-heap run demonstrates that memory is a limiting factor; it does not by itself prove that the
Search logical plan is appropriately sized for production.

Recorded Compose evidence after the reciprocal-reduction workaround was rolled back:

- An earlier text-fixture run lasted 444.28 seconds and ended `1 failed, 12 deselected`; use the reranking case below
  when the first failure stack must include the complete Catalyst heap trace.
- The default PySpark 3.5 `SearchDocuments` reranking case used `-Xmx1g` and first failed after 629.32 seconds at the
  generated feedback-option union, with `java.lang.OutOfMemoryError: Java heap space` in Catalyst `DeduplicateRelations`.
- A semantics-preserving rewrite combines the global and fallback feedback-option lanes in one left join. The rerun no
  longer fails at that union, but still exhausts `-Xmx1g` at the final online/generated parity collect after 495.11 seconds.
- Persisting the offline index inputs without forcing a materialization did not complete within a 576-second bounded run;
  persistence alone is therefore not established as a fix.
- The first rerun of the reranking case exposed a separate missing inference-schema catalog entry before plan analysis;
  the integration fixture now includes the inference result and status schemas so subsequent failures are meaningful.

The larger-heap setting is therefore a diagnostic experiment, not the default fix. The current host has about 3.8 GiB available
to the Compose stack, so larger heaps require a larger Docker Desktop memory allocation and should be repeated only with a
bounded timeout and recorded resource limit.

The Search proving case is not currently heap-safe at the ordinary 1 GiB driver setting. The practical end-user remedy is to
materialize the offline index artifacts as actual tables, files, or checkpointed relations before passing them into the
Search graph. A true `checkpoint()` or `local_checkpoint()` boundary is required to truncate lineage; `cache()` and
`persist()` alone are not sufficient. The branch rewrite reduces one multiplier but does not bound the remaining composed
graph. See the [memory gotcha](troubleshooting/memory/spark_driver_heap_oom.gotcha.md) for the boundary choices.

For Spark Connect, use its separate `STRUCTURE_SPARK_CONNECT_DRIVER_MEMORY` setting. Executor or worker memory changes
are a different experiment: use them only when the error is an executor/shuffle failure, not when the driver fails while
analyzing the plan.
