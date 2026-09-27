# Search integration is slow with a tiny fixture

### Problem (integration): Search takes minutes before collecting a few rows

When: A Search integration test runs both online and generated execution against a small document/query fixture.

Error: The test appears hung while Spark constructs, analyzes, or serializes the query plan. Collection is often
sub-second once construction finishes. In severe cases the driver reports `java.lang.OutOfMemoryError: Java heap space`
or Spark Connect fails while sending a deep plan.

Cause: Search combines retrieval, scoring, validation, and reranking branches over lazy DataFrames. Reusing a branch
after joins, unions, singleton-policy checks, or `require_all`/`require_unique` assertions copies upstream work into
the next branch. A small number of rows does not imply a small query plan. Exposing unused intermediate stage outputs or
enabling intermediate schema validation adds more analysis.

The developer troubleshooting entry is [Performance.trbl.md](Performance.trbl.md). The detailed measurements and
resolution work are tracked in the open
[Search Integration Performance issue](../../dev/issues/I09272601.Search-integration-performance.issue.md).

## See the problem

A focused test has this shape:

    online = SearchDocuments(**inputs).run(session(spark, execution_mode="online")).results
    generated = SearchDocuments(**inputs).run(
        session(spark, execution_mode="generated", generated_package=PACKAGE)
    ).results

The input can be only a few documents and queries, yet both calls construct the composed Search graph before collecting
rows. Run the focused test with the repository's integration runner:

    INTEGRATION_PYTEST_ARGS="-k test_document_search_reranks_bm25_candidates_for_multiple_queries -vv -s --durations=20"

For construction details, add:

    STRUCTURE_PROFILE_QUERY_PLANS=1

Record preparation, online/generated construction, checkpoint, collection, cleanup, and total time. Compare the same
fixture and runtime settings with `STRUCTURE_SEARCH_STAGE_OUTPUTS=0` and with `plan_boundaries="off"` or
`"auto"`.

## Remedies

1. **Measure the phase first.** If collection is fast and construction dominates, changing row assertions or executor
   memory will not address the cause. Run three alternating baseline/candidate repetitions and compare medians.

2. **Avoid work the test does not use.** Focused Search tests should disable unused intermediate stage outputs. Keep
   final input/output validation and strict user-facing safety checks.

3. **Reuse equivalent checks.** Singleton-policy and validation checks should be bounded and reused within one transform
   run instead of copying the same policy query plan into every branch.

4. **Materialize the right batch relation.** Keep `SearchDocuments` as the public component and use
   `FuseDocuments(materialize=~streaming)`. A batch candidate relation may be checkpointed before reranking or
   uniqueness checks reuse it. Streaming frames bypass compiler batch boundaries.

5. **Use shared-plan boundaries deliberately.** `plan_boundaries="auto"` is the normal batch comparison. Use
   `"off"` to reproduce the pre-optimization behavior and `"strict"` only to diagnose boundary placement.

6. **Keep semantics visible.** Preserve ranking, schema, online/generated equality, sentence-splitting UDFs, and strict
   validation failures. A disconnected micro-test or a larger driver heap is not a production fix.

The preliminary Spark 3.5 reranking case spent 140.63 seconds in combined construction while collection stayed below one
second per mode. Guard reuse and earlier materialization reduced the comparable profiled construction to 56.79 seconds,
but the broader backend comparison did not establish a universal speedup. The issue remains open pending repeated
closure benchmarks.

## References

- [Performance troubleshooting](Performance.trbl.md)
- [Performance optimization](../../dev/optimization/Performance.opt.md)
- [Search integration issue](../../dev/issues/I09272601.Search-integration-performance.issue.md)
- [Benchmark evidence](../../dev/issues/I09272601/SearchIntegrationPerformance.bench.md)
- [Search proving heap evidence](../../dev/issues/I09272601/Search-proving-heap.evidence.md)
- [Driver-heap memory gotcha](../memory/spark_driver_heap_oom.gotcha.md)
