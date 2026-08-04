# API Reference

This describes Structure's public, compiler-visible API.

If you are just starting using this library, see [QuickRef.md](../QuickRef.md) for an introduction.

`supported` means the public contract is available now. `planned` needs a more complete type, cardinality, or
determinism contract. `deferred` and `unsupported` deliberately stay outside the current scope.
Structure is not a one-to-one PySpark
wrapper: admitted APIs remain typed, symbolic, capability-checked, explainable, and readable in generated code.

The default target is ordinary PySpark `>=3.5,<4.1`; completed compiler-visible batch features also target Spark
Connect. See [Compatibility.md](../Compatibility.md) for the full target policy,
[APICatalog.md](../APICatalog.md) for the public API catalog and checked coverage table, and
[API Gaps](../dev/Gaps.md) for the developer backlog.

## PySpark 4.1 adoption reference

V11 plans the exact target profile `>=4.1,<4.2`. Ordinary PySpark is the primary variant; Spark Connect is a separate
claim requiring 4.1-specific live evidence. The rows below describe the planned boundary and do not widen the current
default support range until V11 closes.

| PySpark 4.1 surface | Planned Structure status | Contract |
| --- | --- | --- |
| `Column.transform` and typed higher-order additions | design-gated | Symbolic element callback, declared result type, nullability, and row-preserving array semantics |
| Deterministic new scalar/string/binary/collection functions | design-gated | Typed helpers with capability checks and online/generated parity |
| Random/seeded helpers (`random`, `uniform`, `randstr`, `uuid`) | design-gated | Explicit seed and nondeterminism policy required; streaming remains unclaimed |
| `DataFrame.exists` and IN subqueries | planned | Correlation scope, aliases, null behavior, boolean result, and explainable dependencies |
| `DataFrame.lateralJoin` | design-gated | Typed relation output, cardinality, correlation, and streaming contract required |
| Complex observations and sketch aggregates | design-gated | Metric side channels and serialized approximate sketches need separate typed contracts |
| Arrow UDF/UDTF and `transformWithState` | caller-owned-guided/design-gated | Use raw/caller-owned PySpark; Structure does not generate arbitrary worker Python or state stores |

V11's full ledger is in [APICatalog.md](../APICatalog.md#pyspark-41-adoption-v11). The current public baseline remains
ordinary and Connect PySpark `>=3.5,<4.1` until every promoted 4.1 row has capability, diagnostics, tests, and runtime
evidence.

## Core APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| Schemas | supported | `StructType`, SQL types | [Schema reference](Schema.ref.md) |
| Transforms and hooks | supported | DataFrame pipeline | [Transforms API](../api/Transforms.api.md) |
| Expressions | supported | Column and SQL-function subset | [Expressions API](../api/Expressions.api.md) |

**Details And Differences**

- Schema classes own field names, aliases, types, and nullability instead of exposing raw Spark schema objects.
- Transform source is compiler-visible. `@raw` remains the honest boundary for caller-owned PySpark behavior.
- Expression truthiness, raw SQL strings, UDTFs, and arbitrary callback bodies are unsupported. Scalar
  `@special(type="udf")` remains an ordinary-PySpark row-local feature with its warning policy.

## Analytical APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| Joins | supported | DataFrame joins and windowed matching | [Joins API](../api/Joins.api.md) |
| Aggregations and dedupe | supported | `GroupedData` and Window patterns | [Aggregates](../api/Aggregations.api.md) |
| Inline and reusable windows | supported | `Window` and window functions | [Windows API](../api/Windows.api.md) |
| Array/map helpers | supported | Higher-order and map SQL functions | [Collections API](../api/Collections.api.md) |

**Details And Differences**

- Cross joins need explicit `allow_cartesian=True`; right/full join projections must handle nullable sides.
- Aggregates use typed helpers rather than dictionary/list aggregate syntax. Ordered selection is explicit.
- Array and map callbacks return symbolic expressions; they do not run Python code for every row.

## Runtime And Streaming APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| PySpark batch | supported | Spark DataFrames | [Execution](../background/Execution.back.md) |
| Spark Connect batch | supported | Spark Connect DataFrame and Column APIs | [Compatibility.md](../Compatibility.md) |
| Streaming transforms | supported | Streaming-safe shapes | [Streaming API](../api/Streaming.api.md) |
| Generated lifecycle | unsupported | `readStream`, `writeStream` | [Streaming](../background/SparkStreaming.back.md) |

**Details And Differences**

- Callers own streaming sources, sinks, triggers, checkpoints, output modes, and query lifecycle. Event-time
  tumbling/sliding aggregation, session-window aggregation, watermark-bounded dedupe, bounded stream-stream joins,
  stream-static joins, and scalar Python UDFs are compiler-visible transformations; scalar UDFs remain
  ordinary-PySpark only. Use the tested caller-owned recipe in
  [`examples/streams/adoption.py`](../../examples/streams/adoption.py) for source/sink/query lifecycle code.
- Classic-only Spark internals such as SparkContext, RDDs, JVM access, and `_jdf` are unsupported for Spark Connect.

## Planned And Unsupported Surface

The [API Coverage](../APICatalog.md#api-coverage) table classifies the current PySpark transformation baseline, and
the [Streaming](../APICatalog.md#streaming) section classifies the current PySpark Structured Streaming surface. The
rows below remain a compact orientation aid. Loading, storage, actions, and orchestration are not transformation APIs
and stay outside Structure's scope.

| API Area | Status | PySpark Parity | Details |
| --- | --- | --- | --- |
| Struct mutation | supported | `withField`, `dropFields` | Explicit result Schema preserves the exact nested type and aliases. |
| Bitwise expressions | supported | `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not` | Integer/long-only typed Column expressions. |
| Nearest as-of, reordering, extra stats | planned | Advanced joins and analytics | Need admitted contracts. |
| Array variants; generators | partial | `slice`, `sort_array`, `explode`, `posexplode` | Array variants and `posexplode_struct(...)` are supported; other row generators need distinct contracts. |
| Window order; more aggregates | supported | Window functions and aggregate frames | Sprint 14. |
| Collection basics | supported | Core arrays/maps | [Collections API](../api/Collections.api.md) |
| Raw APIs/lifecycle | unsupported | `expr`, raw `WindowSpec`, UDTF | Use hooks; caller owns lifecycle. Scalar `@special(type="udf")` is row-local ordinary-PySpark supported. |

For detailed restrictions, diagnostics, and feature-admission rationale, consult [APICatalog.md](../APICatalog.md),
[API Gaps](../dev/Gaps.md), and the linked reference pages.

## Next Steps

Get started: [GettingStarted.md](../GettingStarted.md)

Reference docs: [Reference.md](../Reference.md)
