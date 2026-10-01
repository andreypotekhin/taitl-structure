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

Focused Compose runs passed ten cases on each ordinary backend:

| Backend | Cases | Test session duration | Slowest per-case call |
| --- | ---: | ---: | ---: |
| PySpark 3.5 | 10 passed | 107.99 s | 8.54 s |
| PySpark 4.0 | 10 passed | 109.39 s | 9.31 s |

The cases cover K=0, 1, 3, and 1,000; null-score ordering and preservation; 16
simultaneous request groups with K=3 and interleaved source rows; four experiment/band scopes under one
request with the same document IDs reused independently; a higher-scoring candidate arriving in a later
micro-batch; an expired candidate; a duplicate below the retained top K
arriving in a later micro-batch; successful processing at the 10,000-ID
validation limit; and rejection at 10,001 IDs. The duplicate case confirms
that validation is not limited to currently retained result rows or a single
input batch. The multi-scope case
confirms duplicate state is scoped by request, experiment, and band. A static
Spark DataFrame window/ranking calculation now serves as a batch oracle for
all successful cases; tied scores verify document-ID ordering. The first
Spark 3.5 null-score run found that Pandas represents nullable doubles as
`NaN`, which breaks Python tuple ordering although Spark later renders the
value as null. The prototype now normalizes null-like scores before ordering
and persistence. The corrected ten-case matrices pass on both ordinary Spark
profiles. Timings include Spark session setup and are not comparative latency
measurements.

The test creates the first two files before starting the stream and sets
`maxFilesPerTrigger=1`, guaranteeing that the normal higher-scoring candidate
and the duplicate-validation candidate arrive in a later micro-batch instead
of relying on test-thread timing. It also normalizes Pandas' `NaN` document-ID
representation, so the K=0 null-row sentinel does not count as a candidate ID.

The five-second idle grace is already far above the desired sub-second
request target, before accounting for micro-batch scheduling, processing,
queueing, and result delivery. Therefore this particular completion mechanism
cannot support the target unless the product contract permits a shorter
grace or Search can obtain a different reliable completion signal. Do not
reduce late-data tolerance or weaken validation just to make the timing pass.

The final callback now emits test-only counters for retained candidates,
validated IDs, and serialized JSON state bytes. Assertions enforce at most K
retained candidates and at most 10,000 validated IDs. In Spark 4.0, the K=1,000
case retained 1,000 candidates, tracked 1,103 IDs, and occupied 45,264
serialized bytes. At the exact validation ceiling, K=3 retained three
candidates, tracked 10,000 IDs, and occupied 100,134 serialized bytes. These
are fixture measurements, not general memory estimates; they do not measure
Spark's checkpoint encoding, Python object overhead, or identifiers longer
than those in this fixture.

### Normal restart probe

The restart case checkpoints after the first input batch, stops the query,
adds a second batch, and resumes the same query with the same checkpoint. The
final rows match the batch oracle on ordinary Spark 3.5 and 4.0. This case uses
Spark's Parquet file sink: the memory sink is test-only and Spark rejects
recovering it from a checkpoint. The probe waits for `lastProgress` to confirm
that batch 1 was processed; `processAllAvailable()` did not return reliably
with processing-time state timeouts because those timeouts continue to drive
micro-batches. This establishes only normal checkpoint restart for this narrow
operation. It does not establish failed-batch replay, sink idempotence, late
event semantics, or Search pipeline recovery.

## Promotion gate still open

Before this probe can justify production Structure extensions, extend it to
cover a range of active-request counts, ties/nulls/missing pairs, and broader
serialized candidate and auxiliary state sizes. The test now reports state
size for K=1,000 and exactly 10,000 IDs. Compare its answers and failure
behavior with an explicit batch oracle. Then prove state recovery, failed-batch
replay, late-event handling,
changed-snapshot/checkpoint incompatibility, and the composed
score-aggregation/fusion/reranking sequence on both ordinary profiles.
Determine how callers observe successful-empty, rejected/expired, and
still-pending requests without changing Search's results schema or adding a
Structure-owned result service.

Only after these contracts hold should the proved narrow operation be
promoted into the PySpark plugin. Spark Connect remains unproved and excluded.
