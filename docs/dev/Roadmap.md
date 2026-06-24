# Roadmap

The roadmap is staged around an IR-first north star. v1 first proves one useful executable transform running both
online and as generated PySpark, then broadens into the contract that lets Structure replace hand-maintained PySpark
boilerplate with strict online execution and optional generated-code workflow. v2 makes that workflow useful for mainstream
analytical pipelines. v3 takes ownership of streaming lifecycle concerns. v4 adds Spark Connect after the ordinary
PySpark contract is stable.

## v1

- Typed schema definitions.
- Transform classes.
- First executable slice for one schema-to-schema transform.
- Online PySpark execution by default through `StructureSession`.
- Builder-style transform invocation.
- Runtime target registry for online and generated PySpark execution.
- Shared PySpark execution semantic contract for online/generated parity.
- Generated PySpark classes.
- Python 3.11+ and PySpark 3.5.x/4.0.x compatibility policy.
- Source-order subtransforms.
- Intermediate schema validation.
- Generated schema constants usable by caller code for reads and pre-write validation/projection.
- Online-materialized Spark schemas available after `.run(session)`.
- Input, intermediate, and output validation modes.
- Explicit data-quality constraint boundary: v1 validation is schema-first and schema-only by default.
- Filtering with `where(...)`.
- Add/drop columns via schema projection.
- Symbolic `join_one(...)`.
- N-step serial joins.
- `@expr_fn` helpers.
- `@before(method, lane=lane)` and `@after(method, lane=lane)` hooks.
- Compiler provenance from source node to IR node to generated PySpark node.
- Static dataflow traceability inferred from IR.
- Streaming-compatible online and generated transforms.
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
- Opt-in data-quality constraint model for accepted values, ranges, uniqueness, referential checks, freshness, and
  row-count policies.
- Phase-bound data-quality constraints for input, intermediate, and output validation.
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
