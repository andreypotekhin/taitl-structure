# Delta Tables in Transforms

A Delta table is persistent state. Structure's ordinary DataFrame outputs describe lazy relations; a Delta mutation
step instead records a typed effect that commits changes to an existing table during `run()`. This lets the normal
source checker and generator examine a mutation before it reaches the native Delta API.

The caller creates the table, provisions native CHECK constraints, configures a Delta-capable Spark session, and
passes the native `DeltaTable` handle. Structure binds it through `delta_input(Schema)` or
`delta_output(Schema)`. A DataFrame cannot stand in for a Delta binding. The feature is currently **implemented;
release-gated** for the isolated ordinary PySpark 4.1.0 / Delta 4.1.0 evidence pair. See
[Delta compatibility](../compatibility/DeltaTables.compat.md) for the admission status.

## Relations and effects

A `delta_input` is a read-only table relation. A `delta_output` is a mutable target and a named result. Both supply
relation parameters to step methods, so predicates and assignments can refer to typed fields. Same-schema mutation
steps return `None` and may record multiple delete, update, merge, or append operations in source order. Successful
execution returns the original caller-provided table handle, not a DataFrame or a metric row.

Structure checks the current table's columns, types, nullability, and declared CHECKs before the first mutation. An
evolving output is checked after its commit. A schema's `constraints = (check(...),)` states what native CHECK metadata
the table must already contain. It is not a request to install that metadata. By default Structure compares normalized
predicates; `delta_check_match="name"`
compares names, and `"off"` skips CHECK comparison. Shape checks remain active. The option can be specified in plugin
configuration, on a transform, or on a step; the closest declaration wins.

## A deliberate schema transition

A same-schema step cannot silently add columns. To change the expected table shape, bind the current table as
`delta_input(OrderV1)`, declare `delta_output(OrderV2)`, annotate the step `-> OrderV2`, and return one
`delta_merge(...).with_schema_evolution().execute()` or
`delta_append(...).with_schema_evolution().execute()` result. Structure validates `OrderV1` before mutation and
`OrderV2` after the native commit. Merge uses Delta's `withSchemaEvolution()`; append applies `mergeSchema=true` to
that writer. The choice is local to the operation, so unrelated writes do not inherit an auto-merge setting.

## Execution and failure

Compilation and generation do not import Delta or start Spark. At execution, Structure validates the native handle
and runs each planned operation once. An effect step is retained even if no later DataFrame reads its result. Fresh
internal table reads after a mutation see the latest committed snapshot. The returned object remains the original
handle; if its `toDF()` was materialized before a mutation, reopen the table for a fresh view.

Each native mutation may commit separately. Structure does not provide a transaction across steps, roll back earlier
commits after a later failure, or retry an uncertain native commit. The native exception remains catchable and carries
step context. Delta mutation steps are batch operations; streaming source/sink lifecycle remains caller-owned.

For signatures and working examples, use the [Delta API](../api/DeltaTables.api.md). The developer
[design](../dev/design/V11DeltaSchemaBoundMutations.design.md) and
[specification](../dev/specifications/V11DeltaSchemaBoundMutations.spec.md) record the compiler and runtime rules.

## Historical reads, CDF, and selective overwrite

`delta_snapshot` and `delta_changes` turn a typed step result into a native Delta reader. The caller still provisions
and binds the table. Snapshot version/timestamp and CDF start/end selectors may be `variable()` values, so the same
compiled transform can serve multiple requests without embedding selector values in generated code. Batch CDF needs
the table property `delta.enableChangeDataFeed=true` and the Spark Delta extension/catalog settings. Structure checks
both before opening the feed. CDF endpoints are inclusive, and Delta history retention still limits which ranges can
be read.

Streaming CDF stays at the normal DataFrame boundary: the caller builds a `readStream` Delta DataFrame with
`readChangeFeed=true`, binds it to a streaming `input(Schema, streaming=True)`, and owns `writeStream`, checkpoints,
and query lifecycle. Structure only compiles the row transformation.

`delta_replace_where(...).execute()` writes a same-schema source into the target's selected slice using Delta's
native `replaceWhere` option. The source and predicate are validated before the commit; Delta enforces that source
rows satisfy the predicate. This operation is a native commit, not a transaction spanning multiple transform steps.
The new snapshot, CDF, and selective-overwrite paths remain release-gated until their pinned live tests pass.
