# V11 Documentation Review

Reviewed on 2026-10-22 and updated with 2026-10-23 release evidence. This record reconciles public guides and current development documents with implemented APIs,
capability admission, and the evidence available at review time. The matrix and build evidence gate passed; the configured default-profile decision and retrospective remain open.

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

The local-link review repaired missing public example links, links to moved plans, and references to absent historical
archives. Current contributor guidance points to maintained canonical design/specification files. Generated examples
and code remain unchanged by this documentation review.

## Release evidence and remaining work

The [2026-10-23 rerun](sprints/Sprint59_V11IntegrationMatrix.md#passing-closeout-rerun-2026-10-23) passed every
ordinary and Connect lane, including isolated provider phases where admitted, and `make build INTEGRATION=1` passed.
The corrected `Alert` schema expectation passed across ordinary 3.5/4.0/4.1. Following that passing matrix, the
configured default profile is `>=3.5,<4.2`; exact-profile APIs remain capability-gated. Managed Databricks sessions, legacy Pandas timeout/cardinality
evidence, broader stateful chains, unreleased Variant operations, and XML/join reordering retain their documented
dispositions. See [Outstanding work](OutstandingTasks.md).

## Review validation

Documentation validation completed on 2026-10-22; release validation completed on 2026-10-23:

- Active Markdown documents: zero missing local targets; changed documents: zero invalid heading anchors.
- Compatibility/documentation consistency suite: 110 passed.
- `make build` after default-profile promotion: formatting, Flake8, and mypy passed; main suite 2,682 passed / 373 skipped; regression suite
  155 passed / 7 skipped; source distribution and wheel built successfully.
- `git diff --check`: passed.
- Fresh live lanes: all six selected lanes and admitted provider phases passed; detailed counts, runtime pins, skip
  reasons, and build suite results are recorded in Sprint 59. `make build INTEGRATION=1` exited successfully.
