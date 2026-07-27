# PySpark Transformation Coverage

This is Structure's checked catalog for its PySpark `>=3.5,<4.1` transformation baseline. It covers typed
transformations over caller-supplied DataFrames—not readers, writers, sessions, catalog/table management, actions,
or streaming lifecycle. The machine-checked inventory and entries are
[`pyspark-transformation-inventory.json`](pyspark-transformation-inventory.json) and
[`pyspark-transformation-coverage.json`](pyspark-transformation-coverage.json).

`supported` is available in the public compiler-visible API. `planned` needs a contract first. `deferred` is not
currently admitted, and `unsupported` is deliberately outside the DSL; use the named alternative.

| PySpark family | Status | Structure spelling or alternative | Contract / notes |
| --- | --- | --- | --- |
| Column comparisons, boolean, arithmetic | supported | Symbolic operators, `between`, `isin`, null predicates | Typed expressions preserve rows. |
| Column bitwise operations | supported | `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not` | Integer/long only; preserves rows. |
| Column string predicates | supported | `contains`, `startswith`, `endswith`, `like`, `ilike`, `rlike` | Typed String predicates preserve rows. |
| Column cast and nested access | supported | `cast`, `try_cast`, attributes, `get_field`, indexing | `try_cast` is capability checked. |
| Struct mutation | supported | `with_field`, `drop_fields` | Requires exact declared struct shape. |
| Column alias and raw `over` | unsupported | Schema fields; typed window helpers | Names and window contracts remain compiler-visible. |
| Conditional/null/string/numeric/temporal functions | supported | `when`, `coalesce`, `nullif`, typed scalar helpers | Exact type and nullability semantics are compiler-visible. |
| Hash and encoding | mixed | Typed hashes; `@raw` for binary encoding | Binary helpers await a public Binary type. |
| JSON/CSV conversion | deferred | `@raw` hook | Inline Schema transport and stable option normalization need a dedicated IR contract. |
| Array construction, lookup, transformation | supported | Typed array helpers | Exact element/nullability and callback rules are validated. |
| Map functions | supported | Typed map helpers | Callback bodies remain symbolic. |
| Generator variants | supported/deferred | `posexplode_struct(...)`; `@raw` for other variants | The admitted generator expands `array<struct>` values with a declared generated scope and ordinal. `explode`, outer forms, and `inline` remain deferred. |
| Projection and filtering | supported | Schema projection and `where` | Schema owns output names and replacement. |
| Joins and hints | supported | Typed join helpers, `relation_alias` | Explicit schema/cardinality; cross needs opt-in; self joins require named aliases. |
| Set operations | supported/deferred | `union_all`, `union_by_name`, `intersect`, `intersect_all`, `subtract`, `except_all`; `@raw` for missing-column union | Exact-schema set operations are supported. Missing-column union remains deferred. |
| Ordering/limit/sample | supported/deferred | `order_by`, `limit`, `offset`; `@raw` for `sample` | Ordered bounds are compiler-visible and batch-only; sampling waits for seed/replacement/reproducibility semantics. |
| Distinct and deduplication | supported | `distinct`, `drop_duplicates` | Watermark form is streaming classified. |
| Grouping and standard aggregates | supported | `group_by`, `rollup`, `cube`, typed aggregates | Declared aggregate output schema. |
| Exact percentile and statistics | mixed | `percentile`, approximate and moment helpers; `@raw` for `mode` | Deterministic mode tie-breaking is unavailable in the 3.5 baseline. |
| Ranking, selection, aggregate windows | supported | Typed window helpers | Raw `WindowSpec` is unsupported. |
| Watermarks | supported | `watermark` | Caller owns source, sink, trigger, output mode, and lifecycle. |
| Session window | supported | `session_window(event_time, gap)` | Static positive gap returns a typed `TimeWindow` grouping key. |
| Bounded stream-stream outer/semi joins | supported | `rowset_join(..., how=Join.LEFT|RIGHT|FULL)`, `exists(...)` | Both streams require watermarks and `event_time_between(...)`; caller uses append mode. |
| Stream-static semi filtering | supported | `exists(...)` | The streaming relation stays on the left; it has no state or output-mode requirement. |

## Excluded Categories

- Readers, writers, storage, catalogs, sessions, and table management are caller-owned.
- Actions and materializers such as `collect`, `count`, `first`, `toPandas`, and `rdd` are outside transformation
  compilation.
- Streaming lifecycle operations—source/sink creation, checkpoints, triggers, output mode, and query control—stay
  caller-owned.
- Python UDF/UDTF registration and arbitrary callback APIs are not coverage targets. Existing scalar
  `@special(type="udf")` remains its documented ordinary-PySpark exception.

See [API Reference](API.ref.md) and [API Gaps](../dev/Gaps.md) for the detailed admission policy.
