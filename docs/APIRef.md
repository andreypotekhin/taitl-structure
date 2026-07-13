# API Reference

This describes Structure's public, compiler-visible API. 

If you are just starting using this library, see [QuickRef.md](QuickRef.md) for introduction.

supported` means the public contract is available now. `planned` has an accepted implementation slice. `future`
is unscheduled. `unsupported` deliberately stays outside the scope. Structure is not a one-to-one PySpark
wrapper: admitted APIs remain typed, symbolic, capability-checked, explainable, and readable in generated code.

The default target is ordinary PySpark `>=3.5,<4.1`; completed compiler-visible batch features also target Spark
Connect. See [Compatibility.md](Compatibility.md) for the full target policy and [API Gaps](dev/Gaps.md) for the
developer backlog.

## Core APIs

| API Area | Status | PySpark Coverage | Reference |
| --- | --- | --- | --- |
| Schemas | supported | `StructType`, SQL types | [Schemas API](api/Schemas.api.md) |
| Transforms and hooks | supported | DataFrame pipeline | [Transforms API](api/Transforms.api.md) |
| Expressions | supported | Column and SQL-function subset | [Expressions API](api/Expressions.api.md) |

**Details And Differences**

- Schema classes own field names, aliases, types, and nullability instead of exposing raw Spark schema objects.
- Transform source is compiler-visible. `@raw` remains the honest boundary for caller-owned PySpark behavior.
- Expression truthiness, raw SQL strings, and arbitrary UDF/UDTF expressions are unsupported.

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
| Ordinary PySpark batch | supported | `SparkSession`, DataFrame, Column | [Streaming API](api/Streaming.api.md) |
| Spark Connect batch | supported | Spark Connect DataFrame and Column APIs | [Compatibility.md](Compatibility.md) |
| Streaming transforms | supported | Row-local, static, bounded-state shapes | [Streaming API](api/Streaming.api.md) |
| Generated streaming lifecycle | planned | `readStream`, `writeStream` | [Streaming API](api/Streaming.api.md) |

**Details And Differences**

- Callers own streaming sources, sinks, triggers, checkpoints, and query lifecycle until the planned orchestration work.
- Classic-only Spark internals such as SparkContext, RDDs, JVM access, and `_jdf` are unsupported for Spark Connect.

## Future And Unsupported Surface

| API Area | Status | PySpark Parity | Details |
| --- | --- | --- | --- |
| Bitwise; struct mutation | future | `bitwise*`, `withField`, `dropFields` | Need typed projection design. |
| Nearest as-of, reordering, extra stats | future | Advanced joins and analytics | Need admitted contracts. |
| Array variants; generators | future | `slice`, `sort_array`, `explode` | Need a row-expansion contract. |
| Window order; more aggregates | planned | Null/multi-key order, aggregate windows | Sprint 14. |
| Collection basics | planned | Size, membership, construction, lookup, map concat | Scheduled for Sprint 15. |
| Raw SQL, windows, UDFs, sinks | unsupported | `expr`, `WindowSpec`, `udf`, `foreachBatch` | Use APIs or hooks. |

For detailed restrictions, diagnostics, and feature-admission rationale, consult [API Gaps](dev/Gaps.md) and the linked
reference pages.

## Next Steps

Get started: [GettingStarted.md](GettingStarted.md)

Reference docs: [Reference.md](Reference.md)
