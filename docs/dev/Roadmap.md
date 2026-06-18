# Roadmap

The roadmap is staged around a compiler-first north star. v1 proves that Structure can replace hand-maintained PySpark
boilerplate with a strict, readable compiler workflow. v2 makes that workflow useful for mainstream analytical
pipelines. v3 takes ownership of streaming lifecycle concerns. v4 adds Spark Connect after the ordinary PySpark
contract is stable.

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
- Compiler provenance from source node to IR node to generated PySpark node.
- Static dataflow lineage inferred from IR.
- Streaming-compatible generated transforms.
- Streaming compatibility report.
- Diagnostic codes with documentation links.
- Setup/configuration doctor.
- TOML configuration with explicit precedence and schema validation diagnostics.
- Incremental-compile architecture hooks, without production cache semantics.

## v2

- Windowing.
- Deduplication helpers.
- Aggregations.
- Advanced grouping.
- Spark higher-order functions for arrays/maps.
- Explicit caching/persistence annotations.
- Repartition/coalesce annotations.
- Join strategy annotations.
- `join_many(...)` and other row-multiplying or existence-oriented join forms.
- Richer static dataflow explain output.
- More detailed performance diagnostics.
- Production incremental compile and cache diagnostics.
- Generated documentation artifacts for schemas and transforms.
- Pytest helper or plugin.

## v3

- Streaming source definitions.
- Streaming sink definitions.
- Generated `readStream` and `writeStream` code.
- Triggers.
- Checkpoints.
- Watermarks.
- Output modes.
- Stateful streaming policies.

## v4

- Spark Connect support.
- Spark Connect compatibility tests.
- Backend capability reporting for ordinary PySpark and Spark Connect targets.
