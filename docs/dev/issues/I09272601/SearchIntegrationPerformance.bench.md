# Search integration performance evidence


This report records bounded Search stage-output and compiler-boundary comparisons, not production performance
guarantees. The only variable changed in the preliminary stage-output pairs below is
`STRUCTURE_SEARCH_STAGE_OUTPUTS`; both runs used the same classic Spark 3.5 image, fixture data, driver settings,
checkpoint policy, and focused test.

Use [Performance.opt.md](../../optimization/Performance.opt.md) for the phase-timing and repetition methodology.

## Preliminary paired run


Command shape, run from the repository root:

    docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml -p structure-integration run --rm -e STRUCTURE_SEARCH_STAGE_OUTPUTS=0|1 -e STRUCTURE_INTEGRATION_TIMEOUT=600 -e 'INTEGRATION_PYTEST_ARGS=-k test_document_search_reranks_bm25_candidates_for_multiple_queries -vv -s --durations=20' structure-integration-pyspark35 bash /workspace/infra/compose/images/pyspark/run-integration.sh pyspark35

The pipe in the explanatory command is a placeholder; run each value separately. The runner's current unquoted
`INTEGRATION_PYTEST_ARGS` expansion means compound `-k "a or b"` expressions are split by the shell, so the
measurements used one test selector per invocation.

| Backend | Stage outputs | Result | Pytest total | Online construction | Generated construction | Online collection | Generated collection | Cleanup |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| classic Spark 3.5 | 0 | 1 passed | 173.16 s | 72.51 s | 69.23 s | 0.77 s | 0.55 s | 0.01 s |
| classic Spark 3.5 | 1 | 1 passed | 181.30 s | 74.52 s | 75.91 s | 0.62 s | 0.38 s | 0.01 s |
| Spark Connect 4.0 | 0 | 1 passed | 73.55 s | 27.59 s | 22.72 s | 0.35 s | 0.32 s | 0.01 s |
| Spark Connect 4.0 | 1 | 1 passed | 76.32 s | 31.53 s | 26.71 s | 0.32 s | 0.23 s | 0.01 s |

The stage-enabled run completed with the same expected rankings and online/generated rows. This one pair is not a
statistically reliable speed estimate: stage-enabled execution was about 4.7% slower by wall clock, with the
variation concentrated in construction. Run three alternating repetitions per backend before drawing a broader
conclusion. Spark emitted truncated-plan warnings at the configured 8,192-character diagnostic limit; those warnings
are expected and do not indicate a failed assertion.

## Scope and limitations


This is one preliminary pair per backend, not a three-repetition median. The text-fixture pair and full default Search
module run remain outstanding. The result does establish that the opt-in path is wired consistently through online
execution, generated compilation, and the shared source cache, and that this Search case remains correct with stage
outputs enabled. It does not justify changing the library default or claiming that exposed stage outputs are the
dominant remaining cost.

The runner reported unrelated orphan-container warnings from older tasks but did not remove them. No other containers
were stopped or deleted during these measurements.

## Default Search-module regression


With the switch omitted, the default stage-output-off path completed the full Spark Connect 4.0 Search module:

    7 passed, 170 deselected in 131.91s

The lane retained its expected online/generated parity and ranking assertions. This is a regression result, not an
additional stage-output comparison; the classic Spark 3.5 full-module lane still needs a fresh run if a current
cross-backend total is required.

## Generic plan-boundary comparison (2026-09-26)


The generic `plan_boundaries` policy is compared with stage exposure fixed at `0`. The current Search plan has
92 steps, 15 selected temporary-view references, and one reliable checkpoint in `fused.materialize_candidates`.
Views do not sever Catalyst lineage. Construction timings include that checkpoint's analysis and execution.

Reproduce the sequential matrix with:

    .venv/bin/python scripts/benchmark_search_boundaries.py --output /tmp/search-boundary-evidence

The script alternates off/auto, auto/off, off/auto for three repetitions of reranking and text-fixture tests on
ordinary 3.5, ordinary 4.0 and Connect 4.0. Each pytest invocation is bounded to 600 seconds. It writes raw logs,
per-run JSON and median summaries; use a fresh output directory. `--summarize` refreshes summaries from saved logs.
Preparation snapshot timings are separate from construction and collection. Text-fixture construction/collection
labels cover its final Scoring comparison, not every preceding transform; total pytest time covers the whole test.

Runtime settings: Spark/PySpark 3.5.0 and 4.0.0; ordinary drivers retain the 1 GiB default, Connect uses 3 GiB and
local[2]. Ordinary lanes use the Compose worker. Shuffle partitions are 1, input parallelism 2, adaptive execution
disabled, automatic broadcast joins disabled, diagnostic plan strings capped at 8192 characters. The fusion
checkpoint, fixture snapshots, UDFs, validation and expected-value assertions are unchanged within each pair.
Runner image identities are `58aee88ba702` (3.5) and `f7e5f66fc1ca` (4.0); source is mounted from the working tree.

Completed focused comparisons (seconds; each successful cell has three repetitions):

| Backend | Case | Policy | Pytest seconds, repetitions 1/2/3 | Median pytest | Median combined construction |
| --- | --- | --- | --- | ---: | ---: |
| ordinary 3.5 | rerank | off | 190.49 / 170.35 / 162.39 | 170.35 | 140.26 |
| ordinary 3.5 | rerank | auto | 174.09 / 169.21 / 175.67 | 174.09 | 142.57 |
| ordinary 4.0 | rerank | off | 169.48 / 198.01 / 282.75 | 198.01 | 164.82 |
| ordinary 4.0 | rerank | auto | 173.72 / 174.91 / 272.64 | 174.91 | 143.03 |
| Connect 4.0 | rerank | off | failed / failed / not repeated | — | — |
| Connect 4.0 | rerank | auto | failed / failed / not repeated | — | — |
| ordinary 3.5 | text | off | 65.62 / 65.42 / 64.69 | 65.42 | 2.02 |
| ordinary 3.5 | text | auto | 66.61 / 64.74 / 59.88 | 64.74 | 1.95 |
| ordinary 4.0 | text | off | 66.41 / 65.48 / 64.87 | 65.48 | 2.74 |
| ordinary 4.0 | text | auto | 70.02 / 66.47 / 65.63 | 66.47 | 2.88 |
| Connect 4.0 | text | off | 46.03 / 45.90 / 45.44 | 45.90 | 2.74 |
| Connect 4.0 | text | auto | 46.74 / 42.71 / 47.28 | 46.74 | 2.64 |

Phase medians, in seconds. O/G means online/generated; combined construction above is the median of per-run sums,
not the sum of separate medians. Preparation covers fixture snapshots; pytest totals additionally include Spark
startup, fixture creation, compilation, assertions, and shutdown. Cleanup lists snapshot/view cleanup; generated
source installation and removal round to 0.00s. These phase medians need not add up to the median total.

| Backend | Case | Policy | Preparation | Construction O/G | Collection O/G | Cleanup snapshot/views |
| --- | --- | --- | ---: | --- | --- | --- |
| ordinary 3.5 | rerank | off | 8.23 | 71.73 / 68.53 | 1.06 / 0.38 | 0.01 / not recorded |
| ordinary 3.5 | rerank | auto | 8.09 | 73.60 / 68.00 | 1.16 / 0.38 | 0.01 / 0.09 |
| ordinary 4.0 | rerank | off | 9.89 | 78.30 / 86.52 | 1.12 / 0.64 | 0.01 / 0.00 |
| ordinary 4.0 | rerank | auto | 8.86 | 72.16 / 71.02 | 0.68 / 0.37 | 0.01 / 0.04 |
| ordinary 3.5 | text | off | 20.25 | 0.99 / 0.96 | 1.13 / 1.01 | 0.03 / 0.00 |
| ordinary 3.5 | text | auto | 19.96 | 1.01 / 0.92 | 1.10 / 1.02 | 0.04 / 0.03 |
| ordinary 4.0 | text | off | 18.82 | 1.35 / 1.37 | 1.13 / 0.98 | 0.03 / 0.00 |
| ordinary 4.0 | text | auto | 19.21 | 1.48 / 1.40 | 1.16 / 0.97 | 0.03 / 0.03 |
| Connect 4.0 | text | off | 14.08 | 1.61 / 1.14 | 1.46 / 1.42 | 0.03 / 0.00 |
| Connect 4.0 | text | auto | 13.23 | 1.21 / 1.43 | 1.21 / 1.18 | 0.03 / 0.34 |

Median complete runner wall times (off/auto) were 171.46/175.15s for ordinary 3.5 rerank, 199.10/176.04s for
ordinary 4.0 rerank, 66.42/65.87s for ordinary 3.5 text, 66.42/67.45s for ordinary 4.0 text, and 47.26/48.04s
for Connect text. Runner wall time includes Compose startup and process cleanup outside pytest.

All 30 successful focused runs preserved their expected rows and online/generated equality. No driver heap
exhaustion was observed in these comparisons. Ordinary 4.0 initially failed to import `examples` in the remote
sentence-splitting worker. Explicit Compose worker PYTHONPATH and worker recreation fixed that infrastructure
failure; those failed runs are excluded. The UDF was not removed or replaced.

There is no reranking speedup on ordinary 3.5: auto's median combined construction is 1.6% higher and total 2.2%
higher. Ordinary 4.0's combined construction median is 13.2% lower and total 11.7% lower, but the long third pair
and large run-to-run spread warrant caution. Some early 3.5 runs overlapped host-side build/golden checks; small
differences are not causal proof. Text-fixture totals change by less than 2% in either direction. A separate auto
pilot passed in 184.91s and is excluded. These results do not establish a general Search speedup.

### Connect checkpoint serialization blocker


Both policies failed twice at `fused.materialize_candidates`, before generated execution or row collection:

    google.protobuf.message.DecodeError:
    Error parsing message with type 'spark.connect.Relation': Exceeded upb_DecodeOptions_MaxDepth

The installed Spark 4.0 client constructs `CheckpointCommand(relation=self._child.plan(session), ...)` at this
point. Fan-out-only boundaries leave a long linear fusion chain before the checkpoint, so reducing shared plans
does not by itself guarantee a shallow checkpoint request. This is a client-side serialization failure, not heap
exhaustion. The previous periodic policy masked a different problem from fan-out reuse; removing it exposes the
problem. A third identical failure per policy was deliberately not run.

A targeted diagnostic placed a temporary-view reference immediately before the existing explicit checkpoint.
It bypassed the protobuf error, but the first probe lost the compiler's column qualifier after replacing the frame.
Restoring that qualifier allowed online Search to finish construction in 43.21s, including a 34.88s checkpoint.
The generated checkpoint then lost its server JVM while an ordinary regression lane was running concurrently.
This is not a successful parity result or a controlled performance measurement; repeat in isolation.

At that point, checkpoint-input staging with qualifier preservation was still a proposal. It was subsequently
approved and implemented, separately from fan-out selection and without an extra eager action. The measurements
below use that implementation; the earlier serialization failures are historical, not results of the new guard work.

## Guard reuse and earlier fusion checkpoints (2026-09-27)

The approved follow-up measures validation growth, reuses equivalent policy checks, and tests earlier materialization
inside `FuseDocuments`. Production changes do not remove `require_all`, `require_unique`, or sentence-splitting UDFs.

Batch `param_join` now collects at most two row structs in Spark, fails unless exactly one exists, and exposes that
row with its original field schema. It no longer cross-joins the policy with a separate count of the policy. An
invocation-local cache reuses checks for identical DataFrame objects and diagnostic scopes; the cache is cleared on
return or exception. Each use projects fresh attributes, avoiding Spark 3.5 conflicts after checkpointing. No data
is collected into Python. Streaming steps still bypass singleton checks.

`FuseDocuments(materialize=True)` now checkpoints ranked lexical and selected vector candidates immediately before
their uniqueness checks, as well as retaining the final selected-candidate checkpoint. This changes one eager write
to three. The additional writes protect both validation and feedback branches; storage remains caller-owned.

### Isolated, equally profiled Spark 3.5 comparison

These are single-run observations, **not medians**. Both used fresh Compose runners, `plan_boundaries=auto`, stage
outputs disabled, identical fixtures/runtime settings, and `STRUCTURE_PROFILE_QUERY_PLANS=1`. No other Spark driver
or full host build ran concurrently. The profiler explicitly requests physical planning through `explain` before
each checkpoint. Subsequent checkpoint time still includes any further planning, serialization, scheduling, and
execution; it is not pure executor time.

| Measurement (seconds) | Original guards, final checkpoint only | Bounded aggregate/reused guards, early + final checkpoints |
| --- | ---: | ---: |
| Online construction | 71.53 | 29.38 |
| Generated construction | 69.10 | 27.41 |
| Combined construction | 140.63 | 56.79 |
| Explain before checkpoints, both modes combined | 11.706 | 6.691 |
| Checkpoint work after explain, both modes combined | 107.896 | 36.938 |
| Online collection | 1.11 | 0.38 |
| Generated collection | 0.42 | 0.30 |
| Whole pytest invocation | 172.55 | 85.49 |

Both passed ranking/equality assertions without heap exhaustion. Combined construction fell about 60%; whole-test
time fell about 50%. This is evidence for the placement and implementation, not a stable cross-backend speedup claim.
The baseline constructed 52 online singleton guards; the final implementation constructed nine per execution mode.
The structural profiler estimates 672 expanded input references before the lexical checkpoint and 236 before the
vector checkpoint. Each becomes one checkpoint source before the uniqueness guard doubles it to two. These estimates
are not row counts or exact Spark optimizer counts. Spark's logged physical-plan string length fell from about
5.49 million characters at the old fusion checkpoint to about 2.00 million at the largest new early checkpoint.
Logging remains capped at 8192 characters; the full text was not retained in Python.

Raw logs: `/tmp/search-guards-baseline35-retry.log` and `/tmp/search-guards-aggregate-early35.log`.
To reproduce the final profiled case from the repository root:

```sh
docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml -p structure-integration run --rm \
  -e STRUCTURE_PROFILE_QUERY_PLANS=1 -e STRUCTURE_SEARCH_STAGE_OUTPUTS=0 -e STRUCTURE_INTEGRATION_TIMEOUT=600 \
  -e 'INTEGRATION_PYTEST_ARGS=-k test_document_search_reranks_bm25_candidates_for_multiple_queries -vv -s --durations=10' \
  structure-integration-pyspark35 bash /workspace/infra/compose/images/pyspark/run-integration.sh pyspark35
```

An intermediate window-based policy prototype passed Search in 149.99s but emitted misleading global-window
warnings. An early-checkpoint prototype exposed conflicting reused `rrf_k` attributes and failed; it is excluded
from the comparison. Live differential checks also caught that the old empty-policy guard raises: the final bounded
aggregate preserves that behavior, including an explicit failure inside the array expression so an empty explode
cannot bypass it. Later Connect testing showed that Spark can prune the old empty cross join there; the new guard
consistently enforces the existing exactly-one-row contract on all three tested backends. The final policy safety
suite passed all ten cases on Spark 3.5 in 44.99s.

### Cross-backend regression evidence

Unprofiled runs select `(test_search.py)or(test_policy_checks)or(test_fusion_materialization)`, with stage outputs
disabled and a 600-second deadline. These include all seven Search tests, policy cardinality/schema/streaming checks,
and fusion parity for valid, empty, and duplicate candidates. They are regression evidence, not isolated medians.

- Spark 3.5: all seven Search tests and ten policy tests passed in the first combined run. Three new fusion tests
  initially had fixture-import and expected-schema mistakes; the corrected tests compare actual schemas with
  checkpointing off/on and passed separately (3 passed in 37.63s). Original aggregation nullability is preserved.
  The added live streaming-bypass case also passed separately (1 passed in 13.53s).
- Spark 4.0: 21 passed in 231.37s, including the additional live streaming-frame bypass case.
- Spark Connect 4.0: the full Search module passed. The first combined run exposed a pre-existing generated-executor
  diagnostic bug: `RDD` in a server stack trace caused a real `REL-E0702` duplicate-key failure to be mislabeled as
  `CONNECT-E2601`. The classifier now ignores the appended JVM stack trace while retaining detection of real
  unsupported client APIs. All 14 policy/fusion safety cases then passed in 29.28s. The full Search module's reranking
  construction was 15.65s online and 13.88s generated (a warmed regression run, not an isolated benchmark).

Logs: `/tmp/search-guards-regression35.log`, `/tmp/search-fusion-safety35-final.log`,
`/tmp/search-guards-regression40.log`, `/tmp/search-guards-regression-connect40.log`, and
`/tmp/search-guards-safety-connect40-final.log`. No successful final run reported driver heap exhaustion.

Final repository verification: `make gold` regenerated the checked-in examples; `make build` passed with
1,947 tests passed / 195 skipped, secondary checks 78 passed / 7 skipped, and mypy clean across 1,278 files.
Both wheel and sdist built successfully. `git diff --check` passed. Build log: `/tmp/search-guards-build-final.log`.
The completed three-point implementation plan is
[archived here](../../planning/past/P09262602.Search-guard-reuse.plan.md).
