# Roadmap

## v1

- Typed schema definitions.
- Transform classes.
- Generated PySpark classes.
- Python 3.11+ and PySpark 3.5.x/4.0.x compatibility policy.
- Source-order subtransforms.
- Intermediate schema validation.
- Filtering with `where(...)`.
- Add/drop columns via schema projection.
- Symbolic `join_one(...)`.
- N-step serial joins.
- `@expr_fn` helpers.
- `@before(method)` and `@after(method)` hooks.
- Basic LDJSON lineage.
- Streaming-compatible generated transforms.
- TOML configuration with explicit precedence and schema validation diagnostics.
- Incremental compiler support.

## v2

- Aggregations.
- Advanced grouping.
- Windowing.
- Deduplication helpers.
- Spark higher-order functions for arrays/maps.
- Explicit caching/persistence annotations.
- Join strategy annotations.
- Optional field-level lineage.
- More detailed performance diagnostics.

## v3

- Spark Connect support.
- Streaming source definitions.
- Streaming sink definitions.
- Generated `readStream` and `writeStream` code.
- Triggers.
- Checkpoints.
- Watermarks.
- Stateful streaming policies.
