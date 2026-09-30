# Streaming Search Feasibility Prototype

Status: preliminary ordinary-PySpark prototype; not a Search design approval

This record captures the first executable feasibility probe for
[I09282601](../I09282601.Search-repeat-query-latency.issue.md). It establishes
that public PySpark grouped state can retain a deterministic, bounded result
and emit it after processing-time inactivity on ordinary Spark 3.5 and 4.0.
It does not establish that the current Search scoring graph can be streamed,
that this completion contract is acceptable for Search, or that streaming
improves repeat-query latency.

## Prototype contract

`tests/integration/pyspark/search/test_search_streaming_prototype.py` reads
candidate events for one request from a caller-owned file source, groups by
request ID, and uses public `GroupedData.applyInPandasWithState` with a
processing-time timeout. As each row is visited, it inserts the candidate in
deterministic order by descending score and ascending document ID, immediately
discarding the lowest-ranked row when the retained set exceeds K. Thus the
candidate portion of the retained state stays at or below K rather than
gathering and sorting all rows from a micro-batch. It emits at most K rows
after an idle grace period and rejects duplicate candidate IDs. An explicit
10,000 unique-ID ceiling bounds duplicate-validation state; exceeding it is an
error. K=0 uses a test-only rank-zero row to make a successful empty answer
observable.

The caller/test owns the file source, checkpoint directory, memory sink,
query start/stop, and cleanup. Spark state is stored as JSON text because a
nested array-of-struct state schema failed to serialize on Spark 4.0's
`applyInPandasWithState` path. This is an implementation workaround in the
prototype, not a proposed Structure public state format.

The prototype exercises a static per-request deadline and processing-time
inactivity timeout. It does not prove event-time watermark completion, a
typed public completion signal, request rejection/expiry reporting, exactly-
once sink delivery, recovery/replay, snapshot compatibility, or Search's
score aggregation and reranking semantics. Empty-answer visibility is only
demonstrated with the test sentinel and must not be copied into Search's
public results schema without an explicit design decision.

## Evidence

Focused Compose runs passed eight cases on each ordinary backend:

| Backend | Cases | Test session duration | Slowest per-case call |
| --- | ---: | ---: | ---: |
| PySpark 3.5 | 8 passed | 87.58 s | 11.90 s |
| PySpark 4.0 | 8 passed | 87.66 s | 11.65 s |

The cases cover K=0, 1, 3, and 1,000; 16 simultaneous request groups with
K=3; four experiment/band scopes under one request with the same document IDs
reused independently; a higher-scoring candidate arriving in a later
micro-batch; an expired candidate; a duplicate below the retained top K
arriving in a later micro-batch; and rejection after the 10,000-ID validation
bound is exceeded. The duplicate case confirms that validation is not limited
to currently retained result rows or one input batch. The multi-scope case
confirms duplicate state is scoped by request, experiment, and band. A static
Spark DataFrame window/ranking calculation now serves as a batch oracle for
all successful cases; tied scores verify document-ID ordering. Timings include
Spark session setup and are not comparative latency measurements.

The five-second idle grace is already far above the desired sub-second
request target, before accounting for micro-batch scheduling, processing,
queueing, and result delivery. Therefore this particular completion mechanism
cannot support the target unless the product contract permits a shorter
grace or Search can obtain a different reliable completion signal. Do not
reduce late-data tolerance or weaken validation just to make the timing pass.

## Promotion gate still open

Before this probe can justify production Structure extensions, extend it to
cover a range of active-request counts and interleaved requests, ties/nulls/
missing pairs, and direct observations of serialized candidate and auxiliary
state sizes. The test currently covers a 16-request point, cross-batch
duplicates, and overflow, but does not yet measure state size. Compare its
answers and failure behavior with an explicit batch oracle. Then prove state
recovery, failed-batch replay, late-event handling,
changed-snapshot/checkpoint incompatibility, and the composed
score-aggregation/fusion/reranking sequence on both ordinary profiles.
Determine how callers observe successful-empty, rejected/expired, and
still-pending requests without changing Search's results schema or adding a
Structure-owned result service.

Only after these contracts hold should the proved narrow operation be
promoted into the PySpark plugin. Spark Connect remains unproved and excluded.
