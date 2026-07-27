# API Gaps

This page tracks PySpark parity gaps, postponed design items, and deliberately unsupported API surface. It is a
developer backlog aid, not a promise that Structure will become a one-to-one PySpark wrapper.

Structure's rule is narrower: admit PySpark features when they can stay symbolic, typed, backend-capability checked,
explainable, testable, and readable in generated code. Everything else should remain in explicit hooks or caller-owned
PySpark until there is a real Structure contract.

See the user-facing summary in [API.md](../API.md) and the unified API status tables in
[APICatalog.md](../APICatalog.md).

## Status

- `implemented`: shipped with the required capability, diagnostic, documentation, and verification evidence for the
  claimed target profile.
- `planned`: accepted direction or reserved API; needs implementation, diagnostics, tests, or docs. A plan may be
  created later.
- `scheduled`: accepted v4 catalog work assigned to a delivery slice. The pre-catalog gap list continues to use
  `planned` until Sprint 17 reclassifies it.
- `deferred`: deliberately postponed because its type, cardinality, determinism, or runtime contract is not yet
  sufficiently specified. It is not an implicit promise for the current release.
- `unsupported`: intentionally outside the compiler-visible DSL, or incompatible with Structure's contract.

## Parity Sources

Track gaps against the default target range in [Compatibility.md](../Compatibility.md):

- PySpark 3.5.x and 4.0.x;
- ordinary PySpark DataFrame/Column APIs;
- Spark Connect for completed compiler-visible batch features.

Consult the official Spark 4.0.1 docs when expanding this page:

- PySpark Column reference:
  <https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.Column.html>
- PySpark SQL functions reference:
  <https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/functions.html>
- PySpark DataFrame join reference:
  <https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html>

The latest Spark docs may be useful for discovery, but features introduced after PySpark 4.0.x should not be marked
`planned` for the current target unless the target range changes.

## V4 Coverage Program

V3's scheduled gaps are complete. V4 now treats this page as input to the checked
[API Coverage catalog](../APICatalog.md#api-coverage), rather than as a list of
isolated surprises. The catalog classifies every relevant PySpark 3.5.x/4.0.x transformation API as supported,
scheduled, deferred, or unsupported and links each supported entry to capability and parity evidence.

The delivery design and first ExecPlan are [V4 Transformation API Coverage](design/V4TransformationApiCoverage.md) and
[P07132601.V4-transformation-api-coverage.plan.md](planning/P07132601.V4-transformation-api-coverage.plan.md).
[V4 Caller-Owned Streaming Migration](design/V4CallerOwnedStreamingMigration.md) and
[P07152602.V4-caller-owned-streaming-migration.plan.md](planning/P07152602.V4-caller-owned-streaming-migration.plan.md)
define the dedicated bounded-streaming transformation slice. Loading, storage, catalog/table management, actions, and
streaming lifecycle ownership are excluded from this program.

## V6 Deferral Discipline

v6 uses the coverage catalog and [APICatalog.md](../APICatalog.md) to schedule small typed PySpark additions, but this
page remains the durable register of postponed and deferred work. The design is
[V6 PySpark API Closure](design/V6PySparkApiClosure.md), and the previous release ledger is now consolidated into
[APICatalog.md](../APICatalog.md). When v6 admits, postpones, or rejects an API, update this page, the coverage
JSON/reference, and the catalog in the same change. Keep the reason, the user-facing boundary (`step`, explicit scalar
UDF, `@raw`, or caller-owned PySpark), and the owning plan together so an omitted API never becomes an implicit promise.

The following v6 candidates remain deferred until their contracts are complete: binary/encoding values, JSON/CSV
inline-schema parsing, row generators, missing-column relation set composition, sampling, deterministic `mode`, and
physical-plan directives. Their relation-operation design/specification is
[Typed Relation Operations](design/TypedRelationOperations.md). Scalar `@special(type="udf")` is already implemented
for ordinary PySpark; its user contract is [Explicit Scalar Python UDFs](specifications/ExplicitScalarUdfs.md). It is
opt-in, type/nullability declared, warning-governed, and excluded from Spark Connect. It is not a substitute for an
unsupported symbolic operation.

The remaining v6 scheduled relation additions are intentionally narrow: bounded parent-hierarchy closure with
deterministic fallback expansion. Implemented P1 relation assertions, branchable typed union, and first-qualified
priority selection exist to replace Search's cohort-band, relevance, and reranking hooks once same-fixture migrations
prove parity. General recursive relations, dynamic-depth traversal, arbitrary graph algorithms, user-defined hierarchy
traversal, and implicit surrogate row identifiers remain deferred until a separate contract defines them.

### Checked v6 register

The following is the durable Gaps-side mirror of the postponed and scheduled rows in
[APICatalog.md](../APICatalog.md). A generic coverage-family entry may remain deferred while this register schedules one
narrower typed capability from that family; no broader API is implied.

| Capability | Status | Owner / current boundary |
| --- | --- | --- |
| Lambda-bound struct field access | implemented | Sprint 24; the two Security reconciliation hooks are typed steps |
| Partitioned `window_max` | implemented | Sprint 24; typed partition/order/frame contract is available, while BM25 remains `@raw` for its separate generator dependency |
| Ordered `collect_list` | implemented | Sprint 24; explicit ascending/descending aggregate keys retain deterministic collection order |
| `exactly_one` validation | implemented | Sprint 24 P0; batch-only ordinary-PySpark relation assertion with generated/online `REL-E0701` failure. Similarity query construction remains `@raw` until its wider multi-output Search migration. |
| Implicit global aggregation | implemented | Sprint 24; aggregate-only steps retain global semantics and enforce empty-input nullability |
| Explicit scalar UDF example | implemented documentation | Sprint 24; documented opt-in ordinary-PySpark exception with warning and Spark Connect boundary |
| `posexplode` over array of structs | implemented | Sprint 25; `posexplode_struct(...)` is available, while Search extraction/scoring remain `@raw` until their same-fixture migrations are completed |
| Other generator forms | deferred | Admit only after a separate cardinality/null/streaming contract |
| Exact-schema relation set composition and self-alias; Search similarity reduction | partial | Sprint 25; exact-schema set operations, branchable lane rejoin, and `relation_alias(...)` are implemented, while Search similarity reduction and relevance-context expansion remain `@raw` until same-fixture migrations are completed |
| Relation order/limit/offset | implemented | Sprint 25; `order_by(...)`, `limit(n)`, and `offset(n)` are compiler-visible. `sample` remains deferred. |
| Branchable typed union | implemented | Sprint 25; independently materialized typed lanes can rejoin through exact-schema `union_all(...)`, while relevance-context expansion remains `@raw` until same-fixture migration is completed |
| `require_unique` / `require_all` / `require_reference` | implemented | Sprint 25; compiler-visible Spark-plan assertions are available, while cohort traversal still waits for bounded hierarchy/fallback operations |
| Bounded parent hierarchy and fallbacks | scheduled | Sprint 25; cohort traversal remains `@raw` |
| First-qualified priority selection | implemented | Sprint 25; `select_first_qualified(...)` is available, while reranking remains `@raw` until same-fixture migration is completed |
| Sampling | deferred | Seed, replacement, and reproducibility contract is incomplete |
| Bounded ordered `scan(...)` | scheduled | Sprint 26; separate typed recurrence plan |
| Binary/encoding; JSON/CSV parsing; Deterministic `mode` | deferred | Retain the documented `@raw` boundary until their type or tie contracts are complete |

## API Catalog

Column, SQL-function, join, aggregation, window, collection, streaming, and v6 release-ledger tables now live in
[APICatalog.md](../APICatalog.md). Keep this page focused on postponed/deferred rationale and use the catalog for
user-facing API status, PySpark parity, and boundaries.

## Admission Checklist

Before moving a gap to implemented, add or update:

- public reference docs and examples;
- backend capability support or an explicit unsupported diagnostic;
- symbolic execution and IR tests;
- generated PySpark rendering tests;
- execution tests when the feature runs online;
- Spark Connect evidence when the feature is claimed for that variant;
- streaming compatibility classification when the feature can receive streaming inputs;
- API catalog rows in [APICatalog.md](../APICatalog.md).
