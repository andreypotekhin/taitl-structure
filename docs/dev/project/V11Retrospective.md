# V11 Retrospective

Recorded on 2026-10-25. V11's release matrix and integration-enabled build passed on 2026-10-23.
The later profile/runtime-validation follow-up was tracked separately and completed on 2026-10-25.

## Delivered

V11 established the exact PySpark 4.1 target and admitted expression/TIME helpers, typed EXISTS/IN/lateral queries,
batch observations, KLL/Theta sketches, Arrow UDFs, and ordinary row/Pandas state processors. Delta and Iceberg
transforms, table inheritance/composition, and explicit spatial namespaces gained independent provider evidence.
The six ordinary/Connect 3.5/4.0/4.1 selections passed and justified the default profile `>=3.5,<4.2`.

The [release matrix](sprints/Sprint59_V11IntegrationMatrix.md#passing-closeout-rerun-2026-10-23) records the actual
selection scope and skip reasons. Ordinary 4.1 used its backend/V11 selection; a passing lane did not imply a broader
selection, managed-provider support, Connect streaming, or every state interface.

## What worked

Typed examples, shared online/generated lowering, and native comparisons made the admissions concrete. Separate
variant/provider gates prevented classic-only results from becoming Connect or managed-runtime claims.
State families retained independent mode, timer, output, and checkpoint evidence. Caller-owned sources, sinks,
checkpoints, and query lifecycle remained a useful limit on the transformation library.

The final matrix rerun corrected the foreachBatch fixture's expected schema to the declared `Alert` handoff rather
than changing the product to satisfy an incorrect assertion. Dated failure and success records preserve that reasoning.

## What to improve

Too many unrelated packages shared Sprint 58. V12 now has twelve focused sprints, with query, ranking, Arrow, state,
composition, and streaming observations scheduled separately. Every feature sprint includes its own evidence;
the final sprint stays dedicated to integrated hardening.

Exact-profile capabilities did not automatically carry forward to the next Spark version. V12 starts with an
operation/variant continuity inventory instead of treating a newer version string as proof of support.

Release evidence and project records drifted after fixes. The sketch plan still described a resolved handoff failure,
several summaries retained an obsolete timeout status, and the original future sprint dates outlived actual delivery.
Reconcile current summaries at closeout, retain dated historical evidence, and archive completed plans with links fixed.

The runtime-check follow-up exposed the cost of one long general test process. Partitioning contracts, examples,
and runtime/concepts preserved the collected test IDs and deadlines while making retries observable.
An idle-sleep interruption also affected wall time. Use command-scoped sleep prevention on macOS, report pytest
and launcher durations separately, and keep Spark runs sequential on the shared worker. Partitioning alone is not
evidence of an overall speed improvement.

## V12 handoff

The profile/runtime guard is implemented and its focused cases pass on all six targets. The partitioned ordinary
3.5 lane passed, followed by the five remaining unfiltered lanes on 2026-10-25. All six selections passed with exact
versions, phase totals, and skip reasons recorded in the archived [runtime-check plan][runtime-plan]. A fresh
2026-10-25 `make build` also passed with 2,714 main tests / 373 skips, 174 follow-up tests / 7 skips, and both
package builds.

| Retained item | Disposition |
| --- | --- |
| Positive 4.2 Variant validation | Sprint 62; no support promotion from rejection tests. |
| Temporal, spatial, sketch, query, ranking, and Arrow depth | Sprints 62–67 with per-target parity. |
| Legacy Pandas timeout and zero/multiple-output evidence | Sprint 68; separate from row/Pandas processor claims. |
| Additional stateful chains and missing-column streaming union | Sprint 69; explicit stage/schema/restart rules. |
| Streaming observations | Sprint 70; caller-owned progress and lifecycle. |
| Compatible 4.2 table-provider and CDC evidence | Early feasibility in Sprint 61; final provider work in Sprint 71. |
| Managed Databricks/Connect spatial evidence | Requires supplied live sessions; retain the exact provider gate. |
| Unreleased Variant mutations, XML, and join reordering | Outside V12's agreed implementation commitments. |

Apache and Sedona still require isolated sessions unless new live evidence proves a safe alternative.
Spark Connect remains batch-only throughout V12. Earlier M0–M6 follow-ups remain separately recorded and are not
closed by V11 or adopted silently into V12.

[runtime-plan]: ../past/planning/P10232601.Profile-diagnostics-and-runtime-version-checks.plan.md
