# Advanced Analytical Operations

Advanced analytical operations are the broader aggregation, window, and collection-helper features planned after the
first v2 analytical slice. They let Structure cover multi-level summaries, explicit window frames, and richer array/map
logic while keeping the work visible to Spark.

The first slice already supports common grouped aggregates, ranking, lag/lead, rolling row metrics, deterministic
latest/earliest selection, exact/subset duplicate removal, and basic array/map callbacks. This page describes the
larger planned surface and the rules users should expect as it becomes available.

## Advanced Grouping

Planned grouping helpers:

- `rollup(...)` for hierarchical totals;
- `cube(...)` for all combinations of grouping keys;
- `grouping_sets(...)` for explicit subtotal levels;
- `grouping_id()` and `is_grouped(field)` to distinguish detail rows from subtotal rows.

Grouping expression keys should be named. Subtotal rows may omit some grouping keys, so output fields for those keys
must be nullable or explicitly filled with a literal label.

## Additional Aggregates

Planned aggregate families include:

- Boolean metrics such as `bool_and(...)` and `bool_or(...)`;
- deterministic `first_value(...)` and `last_value(...)` with explicit ordering;
- statistical metrics such as `stddev(...)`, `variance(...)`, `corr(...)`, and `covar(...)`;
- approximate metrics such as `approx_count_distinct(...)` and `approx_percentile(...)`;
- collection metrics such as `collect_list(...)` and `collect_set(...)` with clear ordering warnings.

Aggregate helpers may also support `where=...` for metric-local filters, and `having(...)` for filtering aggregate
output rows.

## Reusable Windows

Planned window support includes reusable specs:

```python
customer_window = window(
    partition_by=event.customer_id,
    order_by=[event.sequence.asc(), event.event_id.asc()],
    frame=rows_between(preceding(6), current_row()),
)
```

Window frames are explicit. Row frames count physical rows. Range frames use values in the ordering column and require
a compatible order type.

Planned helpers include `percent_rank(...)`, `cume_dist(...)`, `ntile(...)`, `first_value(...)`, `last_value(...)`,
`nth_value(...)`, and window aggregate helpers such as `window_sum(...)` and `window_avg(...)`.

Broad window features remain batch-only until Structure has explicit streaming watermark and state semantics.

## Higher-Order Helpers

Planned array helpers include:

- `arr_exists(...)`;
- `arr_forall(...)`;
- `arr_zip_with(...)`;
- `arr_aggregate(...)`;
- `arr_sort_by(...)`;
- `arr_flatten(...)`;
- `arr_distinct(...)`;
- `arr_position(...)`.

Planned map helpers include:

- `map_transform_keys(...)`;
- `map_zip_with(...)`;
- `map_keys(...)`;
- `map_values(...)`;
- `map_entries(...)`;
- `map_from_entries(...)`.

Callbacks are symbolic. They run once during compilation against typed placeholder expressions and must return typed
Structure expressions or typed literals. They do not run row by row in Python.

Valid callback style:

```python
arr_exists(order.tags, lambda tag: lower(trim(tag)) == "priority")
```

Unsupported callback style:

```python
arr_exists(order.tags, lambda tag: bool(tag))
```

Use symbolic predicates with `&`, `|`, `~`, and `when(...)`. Python boolean control flow, loops, mutation, live Spark
objects, DataFrames, and runtime sessions are not valid inside compiled callbacks.

## Compatibility

Advanced analytical helpers depend on backend support. When a configured PySpark profile or Spark Connect variant
cannot support a helper, Structure should fail during compilation or generation with a backend capability diagnostic
instead of producing generated code that will fail later.

Streaming compatibility is conservative. Advanced grouping and broad windows are batch-only in v2. Row-preserving HOFs
may become streaming-compatible only when target evidence and tests prove the specific helper shape.

## Explain Output

Compact explain should summarize advanced operations without overwhelming routine output:

```text
aggregate(grouping_sets, metrics=5, cardinality=aggregate)
window(percent_rank, partitions=2, order=2, frame=rows)
hof(arr_zip_with, callback=symbolic)
```

Expanded explain should show grouping levels, metric inputs, filter predicates, window partition/order/frame
dependencies, and higher-order callback lineage.

See also: [DSL](DSL.md), [Intermediate representation](IntermediateRepresentation.md),
[PySpark code generation](PySparkCodeGeneration.md), [Streaming compatibility](StreamingCompatibility.md), and
[Backend capabilities](BackendCapabilities.md).
