# Roadmap

## v1

- Schema declarations.
- Transform class discovery.
- Generated PySpark classes.
- Source-order subtransforms.
- Typed intermediate schemas.
- Intermediate schema validation by default.
- `where(...)` filtering.
- Add/drop columns through schema projection.
- `join_one(...)` joins.
- Serial joins across arbitrary numbers of inputs.
- `@expr_fn` expression helpers.
- `@before(method)` and `@after(method)` hooks.
- Clean no-hook generated code.
- Structured compiler errors.
- Config workaround hints in errors where applicable.
- Seed config and optional TOML configuration.
- Basic LDJSON lineage.
- Streaming-compatible generated transforms when caller provides streaming DataFrames.
- CLI check/compile/explain/init.
- Build and CI integration.

## v2

- Typed aggregation subtransforms.
- Advanced aggregation and grouping.
- Window functions.
- Deduplication helpers.
- Higher-order functions for array and map fields.
- Cache and persist hints.
- Join strategy controls and additional Spark hints.
- Optional field-level lineage.
- More optimization diagnostics.

## v3

- Generated streaming source declarations.
- Generated streaming sink declarations.
- Generated `readStream` and `writeStream` code.
- Triggers.
- Checkpoints.
- Watermarks.
- Stateful streaming policies.
- Streaming job packaging patterns.
