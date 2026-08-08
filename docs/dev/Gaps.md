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

The delivery design and first ExecPlan are [API Catalog Design Gates](design/ApiCatalogDesignGates.design.md) and
[P07132601.V4-transformation-api-coverage.plan.md](planning/P07132601.V4-transformation-api-coverage.plan.md).
[Spark Streaming](design/SparkStreaming.design.md) and
[P07152602.V4-caller-owned-streaming-migration.plan.md](../../close/archive/planning/P07152602.V4-caller-owned-streaming-migration.plan.md)
define the dedicated bounded-streaming transformation slice. Loading, storage, catalog/table management, actions, and
streaming lifecycle ownership are excluded from this program.

## V6 Deferral Discipline

v6 uses the coverage catalog and [APICatalog.md](../APICatalog.md) to schedule small typed PySpark additions, but this
page remains the durable register of postponed and deferred work. The design is
[PySpark API Closure](design/PluginArchitecture.design.md), and the previous release ledger is now consolidated into
[APICatalog.md](../APICatalog.md). When v6 admits, postpones, or rejects an API, update this page, the coverage
JSON/reference, and the catalog in the same change. Keep the reason, the user-facing boundary (`step`, explicit scalar
UDF, `@raw`, or caller-owned PySpark), and the owning plan together so an omitted API never becomes an implicit promise.

The following candidates remain deferred in the catalog until their contracts and evidence are complete: missing-column
relation set composition, sampling, and physical-plan directives. V7 delivered Binary encoding, schema-carrying
JSON/CSV parsing, deterministic grouped `mode(...)`, and generator expansion through named delivery slices; their
contracts are [Advanced Analytical Operations](design/AdvancedAnalyticalOperations.design.md)
and [Typed Relation Operations](design/TypedRelationOperations.design.md). Missing-column union, sampling,
and physical-plan directives remain retained backlog. Scalar `@special(type="udf")` is already implemented
for ordinary PySpark and Spark Connect batch; its user contract is [Explicit Scalar Python UDFs](specifications/ExplicitScalarUdfs.spec.md). It is
opt-in, type/nullability declared, warning-governed, and not a substitute for an
unsupported symbolic operation.

The remaining v6 work is cleanup and broader vocabulary, not Search hook retirement. Implemented P1 generators,
relation assertions, parent hierarchy validation, hierarchy closure rows, hierarchy fallback expansion, branchable typed
union, first-qualified priority selection, and typed cohort matcher predicates now cover the Search example's former raw
boundaries. General recursive relations, dynamic-depth traversal, arbitrary graph algorithms, user-defined hierarchy
traversal, and implicit surrogate row identifiers remain deferred until a separate contract defines them.

### Checked v6 register

The following is the durable Gaps-side mirror of the postponed and scheduled rows in
[APICatalog.md](../APICatalog.md). A generic coverage-family entry may remain deferred while this register schedules one
narrower typed capability from that family; no broader API is implied.

| Capability | Status | Owner / current boundary |
| --- | --- | --- |
| Lambda-bound struct field access | implemented | Sprint 24; the two Security reconciliation hooks are typed steps |
| Partitioned `window_max` | implemented | Sprint 24; typed partition/order/frame contract is available, and BM25 no longer needs a raw hook |
| Ordered `collect_list` | implemented | Sprint 24; explicit ascending/descending aggregate keys retain deterministic collection order |
| `exactly_one` validation | implemented | Sprint 24 P0; batch-only ordinary-PySpark/Spark Connect relation assertion with generated/online `REL-E0701` failure. `CreateSimilarityQueries` now uses it with ordered token aggregation and typed query union. |
| Implicit global aggregation | implemented | Sprint 24; aggregate-only steps retain global semantics and enforce empty-input nullability. `CreateIndex` now uses grouped term aggregates plus aggregate-only summaries without a raw hook. |
| Explicit scalar UDF example | implemented documentation | Sprint 24; documented opt-in ordinary-PySpark/Spark Connect exception with warning and declared boundary |
| `posexplode` over array of structs | implemented | Sprint 25; `posexplode_struct(...)` is available, and `Chunking`, `ScoreOverlap`, and `ScoreBm25` now use typed struct-wrapped expansion instead of raw hooks |
| Other generator forms (nested/map/variant) | deferred | Admit only after a separate cardinality/null/streaming contract; primitive scalar-array generators are implemented |
| Exact-schema relation set composition and self-alias | implemented | Sprint 25; exact-schema set operations, branchable lane rejoin, and `relation_alias(...)` are implemented. `ReduceSimilarityScores` now uses them for reciprocal pair matching, exact-schema pair union, and typed per-source ranking. |
| Relation order/limit/offset | implemented | Sprint 25; `order_by(...)`, `limit(n)`, and `offset(n)` are compiler-visible. `sample` remains deferred. |
| Branchable typed union | implemented | Sprint 25; independently materialized typed lanes can rejoin through exact-schema `union_all(...)`. `BuildRelevanceSignals` now uses branch fan-out for global, fallback, and band-scoped impressions/clicks without raw hooks. |
| `require_unique` / `require_all` / `require_reference` / `require_parent_hierarchy` | implemented | Sprint 25; compiler-visible Spark-plan assertions are available, and `ResolveCohortBands` now uses them for bounded band-catalog validation |
| Search cohort band matcher predicates | implemented | Sprint 25; `cross_join(...)`, `where(...)`, `size(...)`, and `array_contains(...)` cover wildcard-or-membership matching inside the typed `ResolveCohortBands` migration |
| Parent hierarchy closure | implemented | Sprint 25; `hierarchy_closure(...)` emits typed bounded `(node, ancestor, depth)` rows without driver collection |
| Bounded parent hierarchy and fallbacks | implemented | Sprint 25; `ResolveCohortBands` now uses `require_parent_hierarchy(...)`, `hierarchy_closure(...)`, and `hierarchy_fallbacks(...)` to retire its raw driver-collection traversal |
| First-qualified priority selection | implemented | Sprint 25; `select_first_qualified(...)` is available. `RerankDocuments` now uses declared candidate keys to select the first eligible query and popularity feedback context without a raw surrogate row ID. |
| Sampling | deferred | Seed, replacement, and reproducibility contract is incomplete |
| Bounded ordered `scan(...)` | implemented | Sprint 26; batch-only ordinary-PySpark recurrence over caller-supplied partitioned timelines, with a positive per-partition bound and `"error"` duplicate-key failure |
| Binary/encoding; JSON/CSV parsing; Deterministic `mode` | implemented | V7 delivered typed Binary fields and encoding helpers, Schema-carrying JSON/CSV conversion, and grouped `mode(value, deterministic=False)` with portable deterministic tie lowering |

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
