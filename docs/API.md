# API

This describes Structure's public, compiler-visible API.

If you are just starting using this library, see [QuickRef.md](QuickRef.md) for an introduction.

`supported` means the public contract is available now. `planned` needs a more complete type, cardinality, or
determinism contract. `deferred` and `unsupported` deliberately stay outside the current scope.
Structure is not a one-to-one PySpark
wrapper: admitted APIs remain typed, symbolic, capability-checked, explainable, and readable in generated code.

The default target is ordinary PySpark `>=3.5,<4.1`; batch features also target Spark
Connect. See [Compatibility.md](Compatibility.md) for the full target policy, [APICatalog.md](APICatalog.md) for the
public API catalog and checked coverage table, and [API Gaps](dev/Gaps.md) for the developer backlog.
For additions on top of PySpark, see [APIExtensions.md](APIExtensions.md). For the detailed
reference page, see [API.ref.md](reference/API.ref.md).

## Core APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| Schemas | supported | `StructType`, SQL types | [Schema reference](reference/Schema.ref.md) |
| Transforms and hooks | supported | DataFrame pipeline | [Transforms API](api/Transforms.api.md) |
| Expressions | supported | Column and SQL-function subset | [Expressions API](api/Expressions.api.md) |

**Details And Differences**

- Schema classes own field names, aliases, types, and nullability instead of exposing raw Spark schema objects.
- Transform source is compiler-visible. `@raw` remains the honest boundary for caller-owned PySpark behavior.
- Expression truthiness, raw SQL strings, UDTFs, and arbitrary callback bodies are unsupported. Scalar
  `@special(type="udf")` remains an ordinary-PySpark row-local feature with its warning policy.

## Analytical APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| Joins | supported | DataFrame joins and windowed matching | [Joins API](api/Joins.api.md) |
| Aggregations and dedupe | supported | `GroupedData` and Window patterns | [Aggregates](api/Aggregations.api.md) |
| Inline and reusable windows | supported | `Window` and window functions | [Windows API](api/Windows.api.md) |
| Array/map helpers | supported | Higher-order and map SQL functions | [Collections API](api/Collections.api.md) |

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
| Generated lifecycle | unsupported | `readStream`, `writeStream` | [Streaming](background/SparkStreaming.back.md) |

**Details And Differences**

- Callers own streaming sources, sinks, triggers, checkpoints, output modes, and query lifecycle. Event-time
  tumbling/sliding aggregation, watermark-bounded dedupe, and scalar Python UDFs are compiler-visible transformations;
  scalar UDFs remain ordinary-PySpark only.
- Classic-only Spark internals such as SparkContext, RDDs, JVM access, and `_jdf` are unsupported for Spark Connect.

## Planned And Unsupported Surface

The [API Coverage](APICatalog.md#api-coverage) classifies the current PySpark transformation baseline. The rows
below remain a compact orientation aid. Loading, storage, actions, and orchestration are not transformation APIs and
stay outside Structure's scope.

| API Area | Status | PySpark Parity | Details |
| --- | --- | --- | --- |
| Struct mutation | supported | `withField`, `dropFields` | Explicit result Schema preserves the exact nested type and aliases. |
| Bitwise expressions | supported | `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not` | Integer/long-only typed Column expressions. |
| Nearest as-of, reordering, extra stats | planned | Advanced joins and analytics | Need admitted contracts. |
| Array variants; generators | partial | `slice`, `sort_array`, `explode`, `posexplode` | Array variants and `posexplode_struct(...)` are supported; other row generators need distinct contracts. |
| Window order; more aggregates | supported | Window functions and aggregate frames | Sprint 14. |
| Collection basics | supported | Core arrays/maps | [Collections API](api/Collections.api.md) |
| Raw APIs/lifecycle | unsupported | `expr`, raw `WindowSpec`, UDTF | Use hooks; caller owns lifecycle. Scalar `@special(type="udf")` is row-local ordinary-PySpark supported. |

For detailed restrictions, diagnostics, and feature-admission rationale, consult [APICatalog.md](APICatalog.md),
[API Gaps](dev/Gaps.md), and the linked reference pages.

## Next Steps

Get started: [GettingStarted.md](GettingStarted.md)

Reference docs: [Reference.md](Reference.md)
