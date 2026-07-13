# Roadmap

The roadmap is staged around an IR-first north star. v1 first proves one useful executable transform running both
online and as generated PySpark, then broadens into the contract that lets Structure replace hand-maintained PySpark
boilerplate with strict online execution and optional generated-code workflow. v2 makes that workflow useful for
mainstream analytical pipelines, promotes Spark Connect for completed batch features, and completes static
caller-owned Spark streaming compatibility diagnostics. v3 closes its scheduled PySpark parity work and hardens
compiler-visible streaming transformations while callers retain lifecycle ownership. v4 expands predictable PySpark
transformation API coverage while loading, storage, and orchestration remain caller-owned.

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
- Source-order step methods.
- Intermediate schema validation.
- Generated schema constants usable by caller code for reads and pre-write validation/projection.
- Online-materialized Spark schemas available after `.run(session)`.
- Input, intermediate, and output validation modes.
- Explicit data-quality constraint boundary: v1 validation is schema-first and schema-only by default.
- Filtering with `where(...)`.
- Add/drop columns via schema projection.
- Symbolic `lookup_join(...)`.
- N-step serial joins.
- `@special(type="expr")` helpers.
- `@raw(lane=lane)` and `@raw(lane=lane)` hooks.
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
- Explicit cache/persist first-slice annotations.
- Repartition/coalesce annotations deferred until their physical-plan contract is specified.
- Broader join strategy annotations deferred until their physical-plan contract is specified.
- `inner_join(...)` and other row-multiplying or existence-oriented join forms.
- Opt-in data-quality constraint model for accepted values, ranges, uniqueness, referential checks, freshness, and
  row-count policies.
- Phase-bound data-quality constraints for input, intermediate, and output validation.
- Compact static dataflow explain output, with richer field-level lineage deferred.
- More detailed performance diagnostics.
- Generated documentation artifacts for schemas and transforms completed in Sprint 10 adoption tooling.
- Pytest helper or plugin completed in Sprint 10 adoption tooling.
- Spark Connect support for completed v1/v2 batch features, using `target_backend = "pyspark"` and
  `target_variant = "spark-connect"`, backed by live online/generated runtime evidence.
- Static first-slice Spark streaming compatibility for caller-owned streaming DataFrames, static lookup side inputs,
  row-local projection/filtering, schema-only validation, and explicit lifecycle/source/sink deferrals. Live
  online/generated runtime evidence remains a v3 entry gate.

## v3

- Planned Column API and SQL function PySpark parity gaps.
- Using-key joins, right/full diagnostics hardening, cross join safety, join strategy directives, and forward as-of
  joins.
- Explicit grouping sets and post-aggregate `having(...)`.
- Window null ordering, multiple order keys, and aggregate windows.
- Collection size/membership, map-key membership, array construction/repeat/union/except, element lookup, safe element
  lookup, and map concatenation.
- Compiler-visible watermarks, stateful dedupe, streaming aggregates, and compatibility diagnostics.
- Caller-owned source, sink, trigger, checkpoint, output-mode, and query-lifecycle guidance.

## v4

- A checked PySpark 3.5.x/4.0.x transformation coverage catalog.
- Broader typed Column, SQL-function, nested-value, relational, join, aggregation, window, and collection coverage.
- Row generators only after an explicit schema-and-cardinality design gate.
- No loading, storage, actions, orchestration, alternative backends, or non-batch Spark Connect work.
