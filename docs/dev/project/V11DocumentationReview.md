# V11 Documentation Review

This record reconciles public guides and current development documents with implemented APIs, capability admission,
and recorded evidence. It covers the V11 feature review, release matrix, default-profile promotion, and subsequent
profile-diagnostic/runtime-check follow-up. Validation dates and remaining work are recorded below.

## Sources and coverage

The review used `src/structure/plugin/pyspark/capabilities/model/PySparkCapabilities.py`, public DSL exports,
`schema/commands/MaterializePySparkSchema.py`, `RenderPySparkSchema.py`, the expression-diagnostic builder, and the
feature specifications and integration cases. It checked the API gateways, Quick Reference, schema/expression/UDF/
aggregation/streaming/spatial references and ledgers, current design/specification records, gate/deferred registers,
roadmaps, backlog, traceability, and Sprints 55–60. Archived plans and dated historical results retain their original
scope; current records point to the implemented replacements.

| Feature | Implemented scope | Maintained public reference |
| --- | --- | --- |
| Column callback and five scalar additions | Exact ordinary/Connect 4.1; random/UUID batch-only | [Transform](../../reference/Transform.ref.md#reusable-expression-helpers-with-columntransform), [Expressions](../../reference/Expressions.ref.md#pyspark-41-scalar-helpers) |
| TIME | Precision 0–6, six helpers, literal/cast/schema support; Spark session flag required | [Schema](../../reference/Schema.ref.md#time-and-spatial-types), [TIME](../../reference/Expressions.ref.md#spark-41-time-values) |
| Relation queries | Correlated EXISTS, uncorrelated IN, inner/left lateral; exact 4.1 batch | [Typed queries](../../reference/Transform.ref.md#typed-relation-queries) |
| Observations | Named typed metric snapshots, caller-triggered actions; exact 4.1 batch | [Observations](../../reference/Expressions.ref.md#typed-batch-observations) |
| KLL/Theta | Exact 4.1 ordinary/Connect batch; ordinary watermarked streaming/restart | [Aggregations](../../api/Aggregations.api.md#sketches-and-bitmaps) |
| Scalar Python UDFs | Admitted ordinary/Connect batch and tested ordinary streaming | [UDF](../../reference/UDF.ref.md#scalar-python-udf) |
| Arrow UDFs | Exact 4.1 batch scalar/iterator/grouped/window; ordinary scalar/iterator streaming | [Arrow UDF](../../reference/UDF.ref.md#arrow-udfs) |
| State processors | Row ordinary 4.1; Pandas ordinary 4.0/4.1; legacy Pandas retains independent evidence gate | [Streaming](../../reference/Streaming.ref.md) |
| Delta/Iceberg | Ordinary 3.5/4.0/4.1 and separately admitted Connect 4.1 | [Tables](../../reference/DeltaIceberg.ref.md) |
| Geospatial | Apache native exact 4.1; isolated Sedona 3.5/4.0/4.1; managed Databricks evidence open | [Geospatial](../../reference/Geospatial.ref.md) |
| Profiles and runtime checks | Broad default plus explicit exact profiles; shared client/engine guard with raise/log/off policies | [Configuration](../../reference/ConfigSchema.ref.md#pyspark-profiles), [Execution](../../reference/Execution.ref.md#runtime-compatibility) |

## Corrections

Public overview tables and the UDF compatibility ledger now distinguish supported scalar/Arrow declarations from
caller-owned UDTFs and native runtime registration. Schema inventories include TIME and KLL/Theta; the API gateway and
Quick Reference link the new features to complete examples. The TIME flag is explicitly a Spark session setting.

Development registers no longer defer implemented sketches or describe Arrow UDFs as wholly caller-owned. The
machine-readable streaming ledger now matches the already admitted Pandas state profile and its completed feature
plan; legacy Pandas state retains its independent evidence gate. Existing consistency checks enforce those distinctions.
The geospatial design/specification records advisory `PYSPARK-W2707` for an exact-4.1 transform containing Apache and Sedona
helpers, its limited static detection scope, and the separate-session WKB-plus-SRID workaround. Managed Databricks
parity remains open. The roadmaps and sprint records distinguish delivered feature work from release checks.

Canonical capability and Connect documents now describe implemented join hints, relation partitioning, typed windows,
generated documentation, and exact-4.0/4.1 Connect checkpointing. Distinct window aggregates remain unsupported.
The governing Delta work package describes the implemented typed Transform workflow and its read/mutation/maintenance
surface in place of the superseded delete-only wrapper proposal.

The local-link review repaired missing public example links, links to moved plans, and references to absent historical
archives. Current contributor guidance points to maintained canonical design/specification files.

The follow-up review replaces rejected root-level target configuration in public background pages and canonical
specifications with plugin tables. It documents the explicit profile selections, broad-profile feature gates, runtime
policy, execution preflight, generated-constructor timing, and direct-import limitation in their configuration and
execution sections. Diagnostic examples and source remedies now point to maintained references, including checkpoint,
capability, streaming, and Connect guidance. Reliable batch checkpoint usage sits beside caching in the Transform
reference.
Expression basics precede version-specific helpers; common aggregate metrics precede Arrow callbacks; KLL/Theta
explanations sit with other sketches. Spatial collision guidance states the static warning's detection limits.
Infrastructure guidance distinguishes mounted-launcher changes from dependency rebuilds, provider isolation, filtered
cases, and phase deadlines. Historical results retain their dates and scope; obsolete current-status statements are
corrected.

## Release evidence and remaining work

The [2026-10-23 rerun](sprints/Sprint59_V11IntegrationMatrix.md#passing-closeout-rerun-2026-10-23) passed every
ordinary and Connect lane, including isolated provider phases where admitted, and `make build INTEGRATION=1` passed.
The corrected `Alert` schema expectation passed across ordinary 3.5/4.0/4.1. Following that passing matrix, the
configured default profile is `>=3.5,<4.2`; exact-profile APIs remain capability-gated. Managed Databricks sessions, legacy Pandas timeout/cardinality
evidence, broader stateful chains, unreleased Variant operations, and XML/join reordering retain their documented
dispositions. The default-profile selection is recorded; the V11 retrospective remains open.

The subsequent runtime-check implementation passes `make build` and its focused matching/mismatching-profile case
on all six pinned ordinary/Connect targets. Its ordinary 3.5 full general phase timed out at 3,600 seconds and 52% of
295 collected tests after passing isolated Iceberg and Delta. That follow-up's full-suite validation remains open and
does not replace the dated release matrix. See [Outstanding work](OutstandingTasks.md) and the
[runtime-check plan](../planning/P10232601.Profile-diagnostics-and-runtime-version-checks.plan.md).

## Review validation

Documentation validation completed on 2026-10-22; release validation completed on 2026-10-23:

- Active Markdown documents: zero missing local targets; changed documents: zero invalid heading anchors.
- Compatibility/documentation consistency suite: 110 passed.
- `make build` after default-profile promotion: formatting, Flake8, and mypy passed; main suite 2,682 passed / 373 skipped; regression suite
  155 passed / 7 skipped; source distribution and wheel built successfully.
- `git diff --check`: passed.
- Fresh live lanes: all six selected lanes and admitted provider phases passed; detailed counts, runtime pins, skip
  reasons, and build suite results are recorded in Sprint 59. `make build INTEGRATION=1` exited successfully.

The follow-up documentation review was validated on 2026-10-24:

- All 308 active Markdown documents: zero missing local targets and zero invalid heading links.
- Documentation paths and anchors in source diagnostic strings: zero missing targets or invalid anchors.
- Focused compatibility, capability, runtime-version, and diagnostic checks: 443 passed.
- `make build`: formatting, Flake8, and mypy (1,453 source files) passed; main suite 2,695 passed / 373 skipped;
  follow-up suite 155 passed / 7 skipped; source distribution and wheel built successfully.
- `git diff --check`: passed. The runtime follow-up's outstanding full live-suite evidence remains recorded above.
