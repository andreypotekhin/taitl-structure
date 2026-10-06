# Delta Tables Compatibility

This page follows the shared compatibility matrix format. Delta integration is optional and separate from the ordinary
DataFrame API baseline. Live admission is tracked independently for ordinary PySpark 3.5, 4.0, and 4.1, plus the exact
PySpark 4.1 Spark Connect Delta package. PySpark 4.2 and other Connect profiles are outside this admission. See the
[Delta API](../api/DeltaTables.api.md) for usage and the
[design](../dev/design/DeltaTables.design.md) and [specification](../dev/specifications/DeltaTables.spec.md) for
contracts.

| Structure API | PySpark parity | Example | PySpark 3.5 | PySpark 4.0 | PySpark 4.1 | Connect 4.1 | Details |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `delta_table(Schema)` | Caller-bound `DeltaTable` mutation target | `orders = delta_table(Order)` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Common same-schema read/write binding. The caller supplies a native Delta table; `-> None` effects may infer a unique target. |
| `delta_input(Schema)` | Read-only row-mutation relation | `orders = delta_input(Order)` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Used for reads/evolution sources and as a restore target when a distinct `delta_output` schema is declared. |
| `delta_output(Schema)` | Evolved result schema | `orders = delta_output(OrderV2)` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Resolves the return schema of an opted-in merge/append evolution; not a mutable input. |
| `delta_delete`, `delta_update` | `DeltaTable.delete`, `DeltaTable.update` | `delta_delete(order, where=...)` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Typed predicates and assignments are compiled to native Delta operations. |
| `delta_merge` | `DeltaTable.merge` | `delta_merge(order, change, on=...)` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Ordered typed clauses; `.execute()` is required and can be returned with a target-schema annotation. |
| `delta_append` | `DataFrameWriter.format("delta").mode("append")` | `delta_append(order, change).execute()` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Append operation; `with_schema_evolution(to=Schema)` scopes `mergeSchema=true` to its writer. |
| `.with_schema_evolution(to=Schema)` | `DeltaMergeBuilder.withSchemaEvolution()` | `delta_merge(...).with_schema_evolution(to=OrderV2)` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Required `to` schema drives compatibility checks. Supports `delta_input` plus matching `delta_output`, or an in-place `delta_table` effect. |
| `delta_replace_where` | Delta overwrite with `replaceWhere` | `delta_replace_where(...).execute()` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Typed same-schema selective overwrite; `.execute()` records the effect. |
| `delta_snapshot` | Delta reader `versionAsOf` / `timestampAsOf` | `delta_snapshot(order, version=18)` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Return annotation selects the ordinary DataFrame output schema. |
| `delta_changes` | Delta reader `readChangeFeed` | `delta_changes(order, starting_version=18)` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | By default, preflight checks the CDF table property and `delta` in session extension/catalog values; `delta_cdf_checks=False` disables these checks. |
| `delta_history`, `delta_detail` | `DeltaTable.history`, `DeltaTable.detail` | `delta_history(order, limit=5)` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Typed subset of evolving native metadata schemas; positive runtime history limit. |
| `delta_generated`, `delta_identity`, `delta_default` | Delta field metadata and writer omission | `delta_columns = (delta_identity(id),)` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Declaration metadata is checked before writing. Identity supports LongType only. |
| `delta_restore` | `restoreToVersion`, `restoreToTimestamp` | `delta_restore(order, version=v).execute()` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | New restore commit; native metrics are not exposed as a transform result. |
| `delta_optimize` | `optimize().executeCompaction/executeZOrderBy` | `delta_optimize(order).execute_compaction()` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Explicit layout effect; optional predicate must use partition columns; metrics are not exposed. |
| `delta_vacuum` | `DeltaTable.vacuum` | `delta_vacuum(order).execute()` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Defaults to 168 hours; shorter retention requires explicit opt-in and respects Delta's native safety check. |
| `check(...)` in `Schema.constraints` | Native Delta CHECK metadata | `check(status != "invalid")` | 3.5.3 / 3.3.3* | 4.0.0 / 4.0.1* | 4.1.0 / 4.1.0* | 4.1.0 / 4.1.0* | Structure validates matching constraints but does not install them. |

`—` means this API is not admitted by the profile. An admitted cell names the pinned PySpark/Delta evidence pair tested
by Structure. Each classic lane's Delta suite reports 31 passed and 3 skipped; the Connect 4.1 suite reports 33 passed
and one ordinary-only JVM inspection skip. Delta upstream compatibility alone does not establish Structure admission.
Connect validates native shape and CHECK metadata but delegates generated, identity, and default behavior to Delta; it
does not inherit ordinary feature-metadata bridge checks. PySpark 4.2 and other Connect profiles remain unadmitted. See
the [Delta Connect admission plan](../dev/planning/past/P10062601.V11-delta-connect-admission.plan.md).

## Caller-owned behavior

Delta table creation, native constraint installation, arbitrary SQL clauses, table administration, and streaming query
lifecycle remain caller-owned. Structure does not retry commits or provide a transaction across multiple transform
steps. CDF still requires `delta.enableChangeDataFeed=true` on the table and a functional Delta session extension and
catalog, even if `delta_cdf_checks=False` disables Structure's preflight. Schema evolution is explicit and scoped to
the individual merge or append operation.
