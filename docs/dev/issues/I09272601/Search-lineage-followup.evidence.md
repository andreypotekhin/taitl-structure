# Search lineage follow-up evidence

## Search Proving-Case Follow-up

The Search proving failures are a separate instance of the same driver-side lazy-lineage class, but the original attribution
to `ReduceSimilarityScores` was incorrect: `SearchDocuments` does not include that transform. Before the boundary follow-up,
its compiled plan contained 91
steps and 77 published stage outputs after the feedback-option branch rewrite, with several joins and unions over expanded
indexed relations. Cross-step common-ancestor fan-out is reported by `PYSPARK-W2702` when its structural cost exceeds
the configured threshold.

On the PySpark 3.5 Compose image with `-Xmx1g`, the document-reranking proving case first failed after 629.32 seconds at
`reranked.merge_feedback_options`, while Catalyst analyzed a generated `DataFrame.union`; the stack reached
`DeduplicateRelations` and reported `OutOfMemoryError: Java heap space`. The semantics-preserving rewrite in
[`rerank.py`](../../../../examples/search/transforms/searching/search_docs/rerank.py) combines global and fallback options in one
left join, preserving the original global-row and fallback-row rules. Focused local tests passed, and the subsequent live run
no longer failed at that union. It still failed at the final online/generated parity `collectToPython` after 495.11 seconds,
with `OutOfMemoryError: Java heap space` (`failed reallocation of scalar replaced objects`).

An experiment that called `persist()` on the offline index inputs without forcing an action did not complete within a
576-second bounded run. This is consistent with the boundary contract: persistence may improve physical reuse, but it does not
truncate the logical plan. Increasing the driver heap is likewise only a diagnostic or postponement; the 3 GiB bounded run
did not produce a proving result.

Initial decision: retain the feedback-option branch rewrite as a safe **Diminish** optimization, but do not claim that Search proving
is fixed and do not insert an automatic checkpoint. A reliable end-user restructuring must introduce a true materialization
boundary before expanded offline artifacts are reused, or redesign the graph around a small stable base relation. A future
cross-step fan-out diagnostic may improve the warning, but it requires a separate false-positive-controlled design and is not
part of the completed materialization feature.

The 2026-09-25 follow-up adds an explicit application-level policy: `FuseDocuments(materialize=True)` eagerly checkpoints
its final candidate relation before reranking reuses it. `SearchDocuments` supplies `materialize=~streaming`, with
`streaming=False` by default. The compiler still inserts no implicit checkpoint. Shared integration snapshots truncate
offline preparation, and Search tests explicitly disable unused stage-output exposure in both execution modes.

With a correctly scoped cached-score fixture, the full reranking test passed on ordinary PySpark 3.5 with a 1 GiB driver
in 181.41 seconds and on Connect 4.0 with a 3 GiB driver in 83.59 seconds. Both retain online/generated parity and the
original ranking assertions. Result collections took less than one second each; eager construction/execution still
dominates. These measurements establish completion, not a claim that all plan-growth or Search performance issues are
solved. The complete text-fixture case passed in 61.74 seconds on ordinary PySpark 3.5 and 47.13 seconds on Connect 4.0;
the combined seven-test Connect Search module passed in 139.34 seconds. The
[completed follow-up plan](../../planning/past/P09252601.Search-integration-lineage.plan.md) records the evidence.



