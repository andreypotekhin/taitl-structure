# Delta Tables in Transforms

A Delta table is persistent state. Structure's ordinary DataFrame outputs describe lazy relations; a Delta mutation
step instead records a typed effect that commits changes to an existing table during `run()`. This lets the normal
source checker and generator examine a mutation before it reaches the native Delta API.

The caller creates the table, provisions native CHECK constraints, configures a Delta-capable Spark session, and
passes the native `DeltaTable` handle. Structure binds it through `delta_table(Schema)` for same-schema reads and
mutations, `delta_input(Schema)` for read-only roles, and `delta_output(Schema)` for explicitly evolved result schemas.
A DataFrame cannot stand in for a Delta binding. The feature is currently **implemented;
release-gated** for the isolated ordinary PySpark 4.1.0 / Delta 4.1.0 evidence pair. See
[Delta compatibility](../compatibility/DeltaTables.compat.md) for the admission status.

## Relations and effects

A `delta_table` is the common relation for reads and same-schema mutations. A typed method may return `None`, with its
unique mutation target inferred from relation resolution, or return a merge operation directly and annotate the method
with the table schema. `@step` disambiguates relations when necessary. A `delta_input` is read-only for row mutations;
it may be a restore target when the return annotation selects a `delta_output` schema. A `delta_output` declares the
result schema for an explicit schema transition and is never a caller-bound table. Relation declarations supply
typed parameters to step methods, so predicates and assignments can refer to typed fields. Successful execution returns
the original caller-provided table handle, not a DataFrame or a metric row.

Structure checks the current table's columns, types, nullability, and declared CHECKs before the first mutation. An
evolving output is checked after its commit. A schema's `constraints = (check(...),)` states what native CHECK metadata
the table must already contain. It is not a request to install that metadata. By default Structure compares normalized
predicates; `delta_check_match="name"`
compares names, and `"off"` skips CHECK comparison. Shape checks remain active. The option can be specified in plugin
configuration, on a transform, or on a step; the closest declaration wins.

## A deliberate schema transition

A same-schema row-write step cannot silently add columns. To evolve its expected table shape, bind the current table as
`delta_input(OrderV1)`, declare `delta_output(OrderV2)`, annotate the step `-> OrderV2`, and return one
`delta_merge(...).with_schema_evolution(to=OrderV2).execute()` or
`delta_append(...).with_schema_evolution(to=OrderV2).execute()` result. The required `to` schema drives static
compatibility checks and must match the declared output. A `delta_table(OrderV1)` binding may instead target an
evolving effect in a `-> None` step without exposing a separate composable output. Structure validates `OrderV1` before mutation and
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

For signatures and working examples, use the [Delta API](../api/DeltaTables.api.md) and the
[Delta recipes](../recipes/DeltaTableMutations.md). The durable developer [design](../dev/design/DeltaTables.design.md)
and [specification](../dev/specifications/DeltaTables.spec.md) record the compiler and runtime rules.

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

## Metadata inspection and table maintenance

`delta_history` and `delta_detail` return typed ordinary relations from a Delta binding. A step annotation selects the
declared result fields, which lets transform code consume recent commits or table location/partition metadata without
hard-coding every vendor column.

`Schema.delta_columns` records explicit expectations for generated, identity, and default fields. Runtime binding
compares those declarations with Delta's own schema metadata before the compiler relaxes insert completeness. Delta
generated expressions and identity attributes reside in Delta log schema metadata; defaults are visible through Spark's
`CURRENT_DEFAULT` column metadata. These declarations do not install table features or make any field optional in
ordinary DataFrame schemas.

Restore, optimize, and vacuum are explicit effect calls. Restore creates a new version; optimize changes physical file
layout; vacuum removes eligible unreferenced files. Structure does not expose native metric rows as transform results.
Vacuum defaults to 168 hours, and a shorter retention requires source opt-in while retaining Delta's native safety
check. Because vacuum can invalidate old time-travel reads, the caller coordinates it with readers and streams. See the
[inspection and maintenance recipe](../recipes/DeltaInspectionAndMaintenance.md).
These paths remain release-gated until the wider V11 admission matrix is complete; see
[Delta compatibility](../compatibility/DeltaTables.compat.md).
