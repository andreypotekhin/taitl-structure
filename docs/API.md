# API

This describes Structure's public, compiler-visible API.

If you are just starting using this library, see [QuickRef.md](QuickRef.md) for an introduction.

`supported` means the public contract is available now. `planned` means implementation is intended but incomplete.
`design-gated` means a contract exists but implementation or evidence is incomplete. `implemented; release-gated`
means the implementation and isolated evidence exist, but the release's wider admission matrix is incomplete.
`caller-owned-guided` means the
caller may use the upstream API around a Structure transform. `streaming-ineligible` means the batch operation requires
materialization for streaming input; `unsupported` deliberately stays outside the current scope.
Structure is not a one-to-one PySpark
wrapper: admitted APIs remain typed, symbolic, capability-checked, explainable, and readable in generated code.

The default target is ordinary PySpark `>=3.5,<4.1`; batch features also target Spark
Connect. See [Compatibility.md](Compatibility.md) for the full target policy, [APICompatibility.md](compatibility/APICompatibility.md)
for family-level compatibility, and [APITracker.md](compatibility/APITracker.md) for detailed parity tracking.
For the detailed narrative reference, see [API.ref.md](reference/API.ref.md).

## API Documentation Map

Use each API reference for its supported helper contracts and its compatibility ledger for the exhaustive
PySpark correspondence and boundary details.

| API area | API reference | Compatibility ledger |
| --- | --- | --- |
| Aggregations and sketches | [Aggregations API](api/Aggregations.api.md) | [Aggregations compatibility](compatibility/Aggregations.compat.md) |
| Collections | [Collections API](api/Collections.api.md) | [Collections compatibility](compatibility/Collections.compat.md) |
| Delta tables | [Delta tables API](api/DeltaTables.api.md) | [Delta tables compatibility](compatibility/DeltaTables.compat.md) |
| Expressions and SQL functions | [Expressions API](api/Expressions.api.md) | [Expressions compatibility](compatibility/Expressions.compat.md) |
| Joins | [Joins API](api/Joins.api.md) | [Joins compatibility](compatibility/Joins.compat.md) |
| Relations | [Relations API](api/Relations.api.md) | [Relations compatibility](compatibility/Relations.compat.md) |
| Schemas and types | [Schemas API](api/Schemas.api.md) | [Schemas compatibility](compatibility/Schemas.compat.md) |
| Streaming | [Streaming API](api/Streaming.api.md) | [Streaming compatibility](compatibility/Streaming.compat.md) |
| Transforms and hooks | [Transforms API](api/Transforms.api.md) | [Transforms compatibility](compatibility/Transforms.compat.md) |
| Windows | [Windows API](api/Windows.api.md) | [Windows compatibility](compatibility/Windows.compat.md) |

Other orientation: [API compatibility overview](compatibility/APICompatibility.md), [API tracker](compatibility/APITracker.md),
[Quick reference](QuickRef.md), and [reference overview](reference/API.ref.md).

## Core APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| Schemas | supported | `StructType`, SQL types | [Schema reference](reference/Schema.ref.md) |
| Transforms and hooks | supported | DataFrame pipeline | [Transforms API](api/Transforms.api.md) |
| Expressions | supported | Column and SQL-function subset | [Expressions API](api/Expressions.api.md) |
| Geospatial | target-gated | Native PySpark 4.1+ and provider namespaces | [Geospatial reference](reference/Geospatial.ref.md) |

**Details And Differences**

- Schema classes own field names, aliases, types, and nullability instead of exposing raw Spark schema objects.
- Transform source is compiler-visible. `@raw` remains the honest boundary for caller-owned PySpark behavior.
- Expression truthiness, raw SQL strings, UDTFs, and arbitrary callback bodies are unsupported. Scalar
  `@special(type="udf")` remains an ordinary-PySpark row-local feature with its warning policy.

## Version compatibility

PySpark 4.1 adoption, feature status, target profiles, and live evidence are maintained in the
[API Compatibility catalog](compatibility/APICompatibility.md#pyspark-41-adoption) and its detailed
[API tracker](compatibility/APITracker.md). The [V11 charter](dev/project-management/V11.md) records project scope and
schedule; these compatibility documents are the source for versioned API claims.

## Analytical APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| Joins | supported | DataFrame joins and windowed matching | [Joins API](api/Joins.api.md) |
| Aggregations and dedupe | supported | `GroupedData` and Window patterns | [Aggregates](api/Aggregations.api.md) |
| Sketches and bitmaps | supported | Typed opaque HLL/Bitmap state | [Aggregations API](api/Aggregations.api.md#sketches-and-bitmaps) |
| Inline and reusable windows | supported | `Window` and window functions | [Windows API](api/Windows.api.md) |
| Array/map helpers | supported | Higher-order and map SQL functions | [Collections API](api/Collections.api.md) |
| Relation operations | supported | Set composition, ordering, assertions, hierarchy, and sampling | [Relations API](api/Relations.api.md) |

**Details And Differences**

- Cross joins need explicit `allow_cartesian=True`; right/full join projections must handle nullable sides.
- Aggregates use typed helpers rather than dictionary/list aggregate syntax. Ordered selection is explicit.
- Array and map callbacks return symbolic expressions; they do not run Python code for every row.

## Runtime And Streaming APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| PySpark batch | supported | Spark DataFrames | [Execution](background/Execution.back.md) |
| Spark Connect batch | supported | Spark Connect DataFrame and Column APIs | [Compatibility.md](Compatibility.md) |
| Streaming transforms | supported | Streaming-safe shapes | [Streaming API](api/Streaming.api.md) |
| Generated lifecycle | unsupported | `readStream`, `writeStream` | [Streaming](background/Streaming.back.md) |

**Details And Differences**

- Callers own streaming sources, sinks, triggers, checkpoints, output modes, and query lifecycle. Event-time
  tumbling/sliding aggregation, session-window aggregation, watermark-bounded dedupe, bounded stream-stream joins,
  stream-static joins, and scalar Python UDFs are compiler-visible transformations; scalar UDFs are batch-supported
  on Spark Connect but remain ordinary-PySpark-only for streaming. Use the tested caller-owned recipe in
  [`examples/streams/adoption.py`](../examples/streams/adoption.py) for source/sink/query lifecycle code.
- Classic-only Spark internals such as SparkContext, RDDs, JVM access, and `_jdf` are unsupported for Spark Connect.

## Planned And Unsupported Surface

The [API compatibility overview](compatibility/APICompatibility.md) classifies the current PySpark transformation
baseline, with detailed family contracts in the linked compatibility ledgers. The rows below remain a compact
orientation aid. General loading, storage publishing, actions, and orchestration stay outside the transform API;
declared [Delta mutation steps](api/DeltaTables.api.md) are its explicit persistent-table exception.

| API Area | Status | PySpark Parity | Details |
| --- | --- | --- | --- |
| Struct mutation | supported | `withField`, `dropFields` | Explicit result Schema preserves the exact nested type and aliases. |
| Bitwise expressions | supported | `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not` | Integer/long-only typed Column expressions. |
| Nearest as-of and extra stats | supported | Advanced joins and analytics | Nearest as-of matching and typed statistics are implemented with explicit tie and target rules. |
| Join reordering | design-gated | Cost-based join planning | No public optimizer-ordering helper; dependency-safe, explainable planning is still required. |
| Array variants; generators | partial | `slice`, `explode`, `posexplode`, `inline` | Typed struct generators are supported; raw or untyped generators need distinct contracts. |
| Window order; more aggregates | supported | Window functions and aggregate frames | Sprint 14. |
| Collection basics | supported | Core arrays/maps | [Collections API](api/Collections.api.md) |
| Raw APIs/lifecycle | unsupported | `expr`, raw `WindowSpec`, UDTF | Use hooks; caller owns lifecycle. Scalar `@special(type="udf")` is row-local ordinary-PySpark supported. |

For detailed restrictions, diagnostics, and feature-admission rationale, consult [APICompatibility.md](compatibility/APICompatibility.md),
[APITracker.md](compatibility/APITracker.md), [Function Gates](dev/gated/Functions.gates.md), and the linked reference pages.

## Extensions Beyond PySpark

These Structure additions make common transform-writing tasks explicit and typed; they are not direct PySpark
methods or functions.

| Capability | Built on | Addition | Reference |
| --- | --- | --- | --- |
| Schema fields in plain Python | Spark SQL types | `boolean`, `date`, `decimal`, `double`, `float`, `integer`, `long`, `map`, `string`, `struct`, `timestamp`, and field-form `array` declare fields that lower to PySpark schemas. | [Schema reference](reference/Schema.ref.md) |
| String options | PySpark string options | Join, as-of, overlap, and tie options accept validated PySpark-style string literals; constants remain available as aliases. | [Relations API](api/Relations.api.md), [Joins API](api/Joins.api.md) |
| Relation cardinality assertion | Lazy aggregate guard | `exactly_one(...)` checks cardinality with `REL-E0701` when Spark evaluates its guard; optimized-away work can skip it. | [Relations API](api/Relations.api.md) |
| Relation integrity assertions | Lazy aggregate guard | `require_unique(...)`, `require_all(...)`, and `require_reference(...)` express typed integrity checks. | [Relations API](api/Relations.api.md) |
| Parent hierarchy validation | Finite self-join validation | `require_parent_hierarchy(...)` checks bounded catalogs and reports `REL-E0706`. | [Relations API](api/Relations.api.md) |
| Priority row selection | Ordered grouping/window pattern | `select_first_qualified(...)` selects one eligible row per declared business key and reports `REL-E0705`. | [Relations API](api/Relations.api.md) |
| Parent hierarchy closure | Iterative relation expansion | `hierarchy_closure(...)` emits typed `(node, ancestor, depth)` closure rows. | [Relations API](api/Relations.api.md) |
| Bounded parent hierarchy fallbacks | Iterative relation expansion | `hierarchy_fallbacks(...)` emits deterministic fallback rows from a bounded path. | [Relations API](api/Relations.api.md) |
| Relation sampling | Spark `DataFrame.sample` | `sample(...)` records reproducible batch sampling. | [Relations API](api/Relations.api.md) |
| Range repartitioning | Spark `DataFrame.repartitionByRange` | `repartition_by_range(...)` records typed, batch-only range partitioning without promising output order. | [Relations API](api/Relations.api.md) |
| Missing-column union | Spark `DataFrame.unionByName` | Nullable/defaulted and nested-struct evolution is supported for batch; array/map and streaming evolution remain gated. | [Relations API](api/Relations.api.md) |
| Bounded ordered scan | Ordered recurrence pattern | `scan(...)` provides batch-only typed state progression over a bounded ordered timeline. | [Relations API](api/Relations.api.md) |
| Deterministic selected-row helpers | Ordered grouping/window patterns | `earliest_by(...)`, `latest_by(...)`, `dedupe_earliest_by(...)`, and `dedupe_latest_by(...)` encode deterministic row-selection policies. | [Aggregations API](api/Aggregations.api.md) |
| Temporal selected-row helpers | As-of join/window patterns | `temporal_one(...)` and `as_of_one(...)` make time direction, tolerance, and tie behavior explicit. | [Joins API](api/Joins.api.md) |

For the exhaustive PySpark comparison, use the compatibility ledgers in the [API documentation map](#api-documentation-map).

## Next Steps

Get started: [GettingStarted.md](GettingStarted.md)

Reference docs: [Reference.md](Reference.md)
