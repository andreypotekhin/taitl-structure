# Search repeat-query preliminary measurements

Status: one-cohort runner smoke; not the acceptance benchmark.

These measurements check whether a distinct request gets faster after the
same Structure runtime has already run Search. They are limited to the cached
lexical-score fixture. They do not represent score-cache misses, the full
production Search workload, or stable latency medians.

## Earlier exploratory sample

An earlier Spark 3.5 direct invocation measured one new distinct request after
an online/generated Search call in the same test:

| Mode | Construction | Collection | Rows |
| --- | ---: | ---: | ---: |
| Online Structure | 22.68 s | 0.34 s | 3 |
| Generated Structure | 22.49 s | 0.35 s | 3 |

The preceding two-query Search call measured 27.77 s online and 24.28 s
generated for construction, then 0.45 s and 0.26 s for collection. That call
includes both queries together and is context only, not a comparable
per-request baseline.

## Four-control runner smoke (2026-09-29)

The new `scripts/benchmark_search_repeat_queries.py` runner completed one
Spark 3.5 cohort with one distinct cached-score query. The first cohort's mode
order was online, generated, handwritten, then prepared handwritten. All four
controls returned three rows with exact schema and row parity. Before the
measured sequence, the fixture ran the existing online/generated two-query
case in the same test.

| Control | Preparation | Per-request construction | Collection |
| --- | ---: | ---: | ---: |
| Structure online | — | 42.91 s | 0.43 s |
| Structure generated | — | 46.08 s | 0.55 s |
| Handwritten batch PySpark | — | 0.74 s | 0.67 s |
| Handwritten prepared PySpark | 0.50 s | 0.38 s | 0.49 s |

The prepared reference caches fixed documents and popularity frames and reads
fixed scoring/reranking policy scalars before the request loop. It preserves
the reference's ranking and validation for this fixture. The first prepared
request, including preparation and collection, took about 1.37 s; subsequent
requests can avoid preparation only while the caller retains the cached frames.
This does not establish a prepared handle or a supported production API, and
it is not an independent full online-scoring implementation.

The test took 192.84 s; total wall time was 193.92 s. That includes fixture and
index setup and the initial online/generated parity call. The runner recorded
commit `2bb21c8362720083b5596fee53e8453a6e78bdf1` and dirty-file SHA-256 values
at cohort start in [`pyspark35-smoke/results.json`](pyspark35-smoke/results.json),
alongside the [integration log](pyspark35-smoke/cached-score-hit-pyspark35-cohort-1.txt)
and [summary](pyspark35-smoke/medians.json).

This one-sample result suggests that constructing the Structure graph, rather
than collecting its three result rows, dominates this cached-score workload.
It is not proof of full Search semantic equivalence, score-miss performance,
or stable per-request latency.

## Spark 4.0 warm parity smoke (2026-09-29)

A later single Spark 4.0 cohort passed all four controls with exact row and
schema parity. This is a warm smoke after earlier diagnostic retries, not a
comparable cold-start sample or a latency median. Search plan construction
varied substantially across those attempts, so only the successful cohort is
reported below; the raw attempts remain in the adjacent `pyspark40-*` folders.

| Control | Preparation | Per-request construction | Collection |
| --- | ---: | ---: | ---: |
| Structure online | — | 45.54 s | 0.48 s |
| Structure generated | — | 45.19 s | 0.48 s |
| Handwritten batch PySpark | — | 0.86 s | 0.66 s |
| Handwritten prepared PySpark | 0.78 s | 0.77 s | 0.45 s |

The initial online/generated fixture construction took 42.45/40.19 s. The
whole test took 217.81 s; runner wall time was 218.97 s. There was one request
per mode, so these values are smoke measurements, not useful medians. See the
[full log](pyspark40-parity/cached-score-hit-pyspark40-cohort-1.txt),
[request records and source hashes](pyspark40-parity/results.json), and
[runner summary](pyspark40-parity/medians.json).

Spark 4.0 initially inferred stricter nullability for four fields in the
handwritten oracle. The oracle now uses the score's already-required
non-null predicate to retain the public Search result's nullable schema while
preserving values; the final four-mode run passed exact schema comparison.
This oracle adjustment does not alter Search production code.

## Spark Connect 4.0 warm parity smoke (2026-09-29)

The same one-request cohort passed on Spark Connect 4.0, including the
previously failing executor package import and exact row/schema parity across
all four controls. Construction and collection were:

| Control | Preparation | Per-request construction | Collection |
| --- | ---: | ---: | ---: |
| Structure online | — | 26.78 s | 0.46 s |
| Structure generated | — | 26.70 s | 0.54 s |
| Handwritten batch PySpark | — | 0.35 s | 0.47 s |
| Handwritten prepared PySpark | 0.25 s | 0.30 s | 0.56 s |

Initial online/generated construction was 32.82/24.23 s; the full test took
155.23 s and runner wall time was 157.00 s. The executor could import the
packaged `examples` and `structure` modules throughout this harness. This is
one warm smoke only, not a median. See the [Connect log](spark-connect40-smoke/cached-score-hit-spark-connect40-cohort-1.txt),
[request records and source hashes](spark-connect40-smoke/results.json), and
[summary](spark-connect40-smoke/medians.json).

## Environment and workload

- Backend: ordinary Spark 3.5 in the local Compose integration environment.
- Fixed caller-owned documents, index, policies, and score snapshots; empty
  vector and streamed-candidate inputs.
- Each measured request had a new query ID and query text. The harness re-keyed
  the fixture's precomputed scores/targets and collected with direct
  `DataFrame.collect()` calls.
- Runtime settings: `plan_boundaries=auto`, stage outputs disabled, unused-step
  pruning disabled, module-level compiled-artifact reuse.
- One cohort and one request per control; no warm-up or repetition protocol.

## Spark 3.5 score-cache-miss and boundary-lifecycle probes (2026-09-29)

Three sequential score-cache-miss requests were run in one Spark session for
online and generated Search. Each request recomputed scores from the supplied
term index. This cohort explicitly closed Structure-owned plan-boundary views
after `collect()` completed, and started by closing views left by the test's
initial parity call. It does not change production lifetime behavior.

| Mode | Construction samples | Median construction | Median collection | Median boundary cleanup |
| --- | --- | ---: | ---: | ---: |
| Online | 57.63, 104.11, 115.88 s | 104.11 s | 1.27 s | 0.20 s |
| Generated | 115.27, 117.67, 119.80 s | 117.67 s | 1.39 s | 0.25 s |

All six results had two rows and exact row/schema parity between online and
generated modes; no heap failure occurred. Each invocation created 15
Structure boundary views, and cleanup reduced the registered count from 15 to
zero. The three online and three generated requests completed in 843.12 s of
pytest time (847.01 s wall time). Raw timings, cleanup counts, and source
hashes are in [`pyspark35-boundary-release/results.json`](pyspark35-boundary-release/results.json)
and its [log](pyspark35-boundary-release/score-cache-miss-pyspark35-cohort-1.txt).

A prior same-backend cohort did not close views between requests. Its online
construction samples were 44.78, 50.68, and 87.10 s; generated samples were
117.93, 124.12, and 223.96 s. It accumulated 15 views per call, from 42 at
the first measured request to 132 at the end. This is not a controlled causal
A/B comparison: the cleanup cohort ran later in a different JVM session and
began after explicitly releasing 42 views. It does show that registered-view
accumulation is not the sole cause: with no registered boundaries after each
request, construction remained very expensive. A repeated run with alternating
lifecycle order is needed before claiming cleanup improves construction. Do
not change production boundary lifetime based on this test-only experiment.

See the [execution plan](../../planning/P09282601.Search-repeat-query-streaming.plan.md)
for next diagnostics. The issue remains open.

### Timing-only checkpoint profile

A separate one-request score-cache-miss sample enabled
`STRUCTURE_PROFILE_QUERY_PLANS=timing` and released boundaries after each
collection. This mode did not call `explain()` or add actions; checkpoint
times are nested inside the measured construction time.

| Mode | Construction | Checkpoint 1: lexical validation | Checkpoint 2: vector validation | Checkpoint 3: fused candidates | Collection |
| --- | ---: | ---: | ---: | ---: | ---: |
| Online | 22.76 s | 13.35 s | 4.73 s | 0.19 s | 0.38 s |
| Generated | 22.65 s | 13.22 s | 4.76 s | 0.19 s | 0.35 s |

The three eager checkpoints account for about 80% of this request's
construction in both modes. Singleton-policy checks and `require_all` guards
in the same profile were generally 0.005–0.093 s individually; they are not
the dominant measured cost. This is a single warm Spark 3.5 sample, excluding
separate `explain()` time, and should not be compared directly to prior
explain-profiled runs. The initial fixture parity call in the same log also
shows checkpoint timing and is not part of the repeat-request rows. See the
[full profiled log](pyspark35-checkpoint-profile/score-cache-miss-pyspark35-cohort-1.txt)
and [runner summary](pyspark35-checkpoint-profile/medians.json).

M1 still needs query-scoped score-cache misses, three independent cohorts, and
the planned 20-request runs on ordinary Spark 3.5/4.0 and supported Connect
profiles. The Spark 4.0 result above is only one warm parity smoke. See the
[execution plan](../../planning/P09282601.Search-repeat-query-streaming.plan.md).
