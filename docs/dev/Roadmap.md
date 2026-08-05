# Roadmap

The roadmap is staged around an IR-first north star. The initial release proved one useful executable transform running
both through execution and as generated PySpark. The analytical expansion made that workflow useful for mainstream
pipelines, while later hardening broadened PySpark parity and caller-owned streaming compatibility. Target ownership is
now explicit through a public Plugin API: Core continues to orchestrate every workflow, the bundled PySpark plugin
supplies target-specific service facets, and external wheels can supply equivalent plugin integrations.

## Foundations

- Typed schema definitions.
- Transform classes.
- First executable slice for one schema-to-schema transform.
- Online PySpark execution by default through `StructureSession`.
- Builder-style transform invocation.
- Runtime target registry for execution and generated-code execution.
- Shared PySpark execution semantic contract for execution/generated-code parity.
- Generated PySpark classes.
- Python 3.11+ and PySpark 3.5.x/4.0.x compatibility policy.
- Source-order step methods.
- Intermediate schema validation.
- Generated schema constants usable by caller code for reads and pre-write validation/projection.
- Online-materialized Spark schemas available after `.run(session)`.
- Input, intermediate, and output validation modes.
- Explicit data-quality constraint boundary: validation is schema-first and schema-only by default.
- Filtering with `where(...)`.
- Add/drop columns via schema projection.
- Symbolic `lookup_join(...)`.
- N-step serial joins.
- `@special(type="expr")` helpers.
- `@raw(lane=lane)` and `@raw(lane=lane)` hooks.
- Compiler provenance from source node to IR node to generated PySpark node.
- Static dataflow traceability inferred from IR.
- Streaming-compatible execution and generated-code execution transforms.
- Streaming compatibility report.
- Diagnostic codes with documentation links.
- Setup/configuration doctor.
- TOML configuration with explicit precedence and schema validation diagnostics.
- Incremental-compile architecture hooks, without production cache semantics.

## Analytical Platform

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
- Spark Connect support for completed batch features, using `plugin.default = "pyspark"` and
  `plugin.pyspark.variant = "spark-connect"`, backed by live execution/generated-code runtime evidence.
- Static first-slice Spark streaming compatibility for caller-owned streaming DataFrames, static lookup side inputs,
  row-local projection/filtering, schema-only validation, and explicit lifecycle/source/sink deferrals. Live
  execution/generated-code runtime evidence remains a later entry gate.

## Streaming And API Coverage

- Planned Column API and SQL function PySpark parity gaps.
- Using-key joins, right/full diagnostics hardening, cross join safety, join strategy directives, and forward as-of
  joins.
- Explicit grouping sets and post-aggregate `having(...)`.
- Window null ordering, multiple order keys, and aggregate windows.
- Collection size/membership, map-key membership, array construction/repeat/union/except, element lookup, safe element
  lookup, and map concatenation.
- Compiler-visible watermarks, stateful dedupe, streaming aggregates, and compatibility diagnostics.
- Caller-owned source, sink, trigger, checkpoint, output-mode, and query-lifecycle guidance.

## Coverage Hardening

- A checked PySpark 3.5.x/4.0.x transformation coverage catalog.
- Broader typed Column, SQL-function, nested-value, relational, join, aggregation, window, and collection coverage.
- Caller-owned streaming migration for session-window aggregation, bounded stream-stream outer and semi joins, and
  stream-static left-semi joins; sources, sinks, checkpoints, triggers, output-mode application, and query lifecycle
  remain caller-owned in Sprint 18.
- Row generators only after an explicit schema-and-cardinality design gate.
- No loading, storage, actions, orchestration, alternative backends, or non-batch Spark Connect work.
- A final hardening sprint after all coverage sprints, with no new feature scope.

## Plugin Architecture

- Core-orchestrated schema, compilation, execution, generation, serialization, capability, and diagnostic workflows,
  with private target-local replacement of a compatible individual engine when an advanced plugin requires it.
- Public, versioned `PluginAPI` façades with symmetric Core/plugin API negotiation.
- Metadata-only discovery of one plugin per plugin name installed through Python package entry points.
- Exactly one target per transform or composed pipeline, with different transforms in one project allowed to select
  different installed plugins.
- Target-owned plugin DSLs: field definitions, expressions, joins, aggregations, and other target APIs.
- Bundled PySpark behavior moved behind the same public Plugin API available to external plugins.
- Vendor-owned import packages for external plugin DSLs.
- An internal finite-iterable wheel proving discovery, isolation, execution, serialization, and conformance without
  receiving a public product-support claim.
- Immediate removal of target-owned names from the `structure` package root.

## Current Adoption Focus

- Adopt PySpark `>=4.1,<4.2` with ordinary PySpark as the primary target variant and separately evidenced Spark Connect
  parity.
- Maintain a versioned 4.0-to-4.1 API inventory with one status, owner boundary, contract, diagnostic, test, and
  evidence path for every reviewed addition.
- Admit only typed, symbolic, explainable 4.1 expression and relational APIs; keep arbitrary Python, readers/writers,
  session/catalog, declarative-pipeline, SQL-scripting, and streaming-state/lifecycle families explicitly gated or
  caller-owned.
- Extend the six-lane live integration matrix to PySpark 4.1 and retain 3.5/4.0 regression evidence.
- Close the adoption work only after catalog/reference reconciliation, generated-artifact freshness, live parity
  evidence, and `make build`.
