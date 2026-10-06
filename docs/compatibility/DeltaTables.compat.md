# Delta Tables Compatibility

This page follows the shared compatibility matrix format. Delta integration is release-gated and separate from the
ordinary DataFrame API baseline. Isolated evidence covers classic PySpark 4.1.0 with Delta 4.1.0; the broader V11
admission matrix and Spark Connect are not claimed. See the [Delta API](../api/DeltaTables.api.md) for usage and the
[design](../dev/design/DeltaTables.design.md) and [specification](../dev/specifications/DeltaTables.spec.md) for
contracts.

| Structure API | PySpark parity | Example | PySpark 3 | PySpark 4 | Details |
| --- | --- | --- | --- | --- | --- |
| `delta_table(Schema)` | Caller-bound `DeltaTable` mutation target | `orders = delta_table(Order)` | — | 4.1 only* | Common same-schema read/write binding. The caller supplies a native Delta table; `-> None` effects may infer a unique target. |
| `delta_input(Schema)` | Read-only Delta relation | `orders = delta_input(Order)` | — | 4.1 only* | Used for snapshot/CDF reads and as the source binding for explicit schema evolution. |
| `delta_output(Schema)` | Evolved result schema | `orders = delta_output(OrderV2)` | — | 4.1 only* | Resolves the return schema of an opted-in merge/append evolution; not a mutable input. |
| `delta_delete`, `delta_update` | `DeltaTable.delete`, `DeltaTable.update` | `delta_delete(order, where=...)` | — | 4.1 only* | Typed predicates and assignments are compiled to native Delta operations. |
| `delta_merge` | `DeltaTable.merge` | `delta_merge(order, change, on=...)` | — | 4.1 only* | Ordered typed clauses; `.execute()` is required and can be returned with a target-schema annotation. |
| `delta_append` | `DataFrameWriter.format("delta").mode("append")` | `delta_append(order, change).execute()` | — | 4.1 only* | Append operation; `with_schema_evolution()` scopes `mergeSchema=true` to its writer. |
| `.with_schema_evolution()` | `DeltaMergeBuilder.withSchemaEvolution()` | `delta_merge(...).with_schema_evolution()` | — | 4.1 only* | Requires an explicit `delta_input` source, `delta_output` result, direct returned operation, and statically compatible schemas. |
| `delta_replace_where` | Delta overwrite with `replaceWhere` | `delta_replace_where(...).execute()` | — | 4.1 only* | Typed same-schema selective overwrite; `.execute()` records the effect. |
| `delta_snapshot` | Delta reader `versionAsOf` / `timestampAsOf` | `delta_snapshot(order, version=18)` | — | 4.1 only* | Return annotation selects the ordinary DataFrame output schema. |
| `delta_changes` | Delta reader `readChangeFeed` | `delta_changes(order, starting_version=18)` | — | 4.1 only* | Requires table CDF property and Spark session extension/catalog configuration. |
| `check(...)` in `Schema.constraints` | Native Delta CHECK metadata | `check(status != "invalid")` | — | 4.1 only* | Structure validates matching constraints but does not install them. |

`—` means this API is not admitted by the current project profile. `4.1 only*` means isolated classic runtime evidence
exists for PySpark 4.1.0 and Delta 4.1.0, while public release admission is pending. No Spark Connect claim is made.

## Caller-owned behavior

Delta table creation, native constraint installation, arbitrary SQL clauses, table administration, and streaming query
lifecycle remain caller-owned. Structure does not retry commits or provide a transaction across multiple transform
steps. CDF additionally requires `delta.enableChangeDataFeed=true` on the table and the Delta session extension and
catalog. Schema evolution is explicit and scoped to the individual merge or append operation.
