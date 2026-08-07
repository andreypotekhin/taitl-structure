# Reference

This page collects public reference material. These documents are more detailed than the quick guides and define the
behavior Structure aims to keep stable.

For API orientation, start with [API.md](API.md). For function-by-function support, PySpark parity, examples, and
discrepancies, use the [API catalog](APICatalog.md) and [API reference](reference/API.ref.md).

- [Schema reference](reference/Schema.ref.md): declarations, type semantics, inheritance, construction, validation,
  runtime schemas, and data-quality boundaries.
- [Transform reference](reference/Transform.ref.md): transform declarations, steps, bindings, projections, hooks,
  composition, streaming compatibility, and execution boundaries.
- [Aggregations reference](reference/Aggregations.ref.md): grouped metrics, selected rows, deduplication, windows,
  and higher-order collection operations.
- [Join reference](reference/Join.ref.md): lookup, rowset, existence, temporal, as-of, and Cartesian joins.
- [Configuration reference](reference/ConfigSchema.ref.md): files, precedence, target selection, validation, and CI
  settings.
- [CLI reference](reference/CLI.ref.md): initialization, checking, compilation, explain, diff, profile, and cleanup.
- [Execution reference](reference/Execution.ref.md): sessions, deferred invocation, result access, execution modes,
  validation, and application-controlled Spark lifecycle.
- [Streaming reference](reference/Streaming.ref.md): streaming inputs, compatibility checks, watermarks, stateful
  operations, joins, lifecycle handoff, and corrective diagnostics.
- [Search example reference](reference/Search.ref.md): chunking, indexing, lexical scoring, presentation, similarity,
  feedback, and evaluation.
- [Store example reference](reference/Store.ref.md): catalog, recommendations, demand, fulfillment, reconciliation,
  and analytics workflows.
- [Search example specifications](examples/search/Readme.md): focused boundaries, decisions, and deferred
  alternatives.
- [Store example specifications](examples/store/Readme.md): focused boundaries, decisions, and deferred alternatives.

Detailed topic material remains in [background](background/).
