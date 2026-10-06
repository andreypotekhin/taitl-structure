# Delta Tables Compatibility

Delta mutations are **implemented; release-gated**. Isolated live tests cover ordinary PySpark 4.1.0 with
`delta-spark` 4.1.0, including online/generated parity and explicit merge/append schema evolution. The V11 PySpark
4.1 capability profile and broader integration matrix have not yet passed the public support gate. Delta is an
optional runtime dependency, not part of Structure's default PySpark `>=3.5,<4.1` target.

| Runtime or mode | Status | Evidence and boundary |
| --- | --- | --- |
| Ordinary PySpark 4.1.0 + Delta 4.1.0 | implemented; release-gated | Isolated live mutation and online/generated parity tests; wider V11 admission pending. |
| Default PySpark `>=3.5,<4.1` | no Delta support claim | The default profile remains unchanged; no 3.5/4.0 Delta matrix is admitted. |
| Spark Connect | design-gated | Native Delta binding and mutation behavior need separate evidence. |
| Structured Streaming | unsupported | Delta effect steps require batch inputs and execute native table mutations. |

## API correspondence

| Structure API | Native Delta/PySpark API | Boundary |
| --- | --- | --- |
| `delta_input(Schema)` | `DeltaTable.toDF()` for relation reads | Caller supplies an existing native `DeltaTable`; Structure validates its shape and CHECK metadata. |
| `delta_output(Schema)` | Native `DeltaTable` mutation target | Caller supplies the table for same-schema effects; result preserves its object identity. |
| `check(predicate, name=...)` | Delta `CHECK` table property | Expected metadata is verified, never installed or modified. |
| `delta_delete(target, where=...)` | `DeltaTable.delete(condition)` | Requires an explicit typed Boolean predicate. |
| `delta_update(target, where=..., set=...)` | `DeltaTable.update(condition, set)` | Assignments are typed target-schema values. |
| `delta_merge(target, source, on=...)` | `DeltaTable.merge(...)` builder | Ordered matched, unmatched, and unmatched-by-source data mutation clauses; symbolic expressions only. |
| `delta_append(target, source).execute()` | `DataFrameWriter.format("delta").mode("append")` | Resolves the bound table location; append is a native commit. |
| `delta_snapshot(target, version=... / timestamp=...)` | Delta `versionAsOf` / `timestampAsOf` reader options | Direct result of a typed step; runtime scalar selectors are invocation-bound. |
| `delta_changes(target, starting_version=... / starting_timestamp=...)` | Delta `readChangeFeed` reader options | Requires the table property and Delta Spark session configuration; endpoints are inclusive. |
| `delta_replace_where(target, source, where=...).execute()` | Delta `replaceWhere` overwrite option | Same Structure Schema and target-only predicate; live evidence for this addition is pending. |
| `variable(type, default=...)` | Runtime scalar binding | Compiled source refers to the variable; values are supplied per invocation and do not specialize artifacts. |
| Merge `.with_schema_evolution()` | `DeltaMergeBuilder.withSchemaEvolution()` | Requires a distinct return-typed `delta_output` schema. |
| Append `.with_schema_evolution()` | Writer `.option("mergeSchema", "true")` | Applies to this append, not the Spark session. |

Structure checks the current table schema before mutations and the declared new schema after an evolving commit.
`delta_check_match="expression"` is the default; `"name"` and `"off"` relax native CHECK comparison but never
disable shape validation. Raw SQL clauses, Delta table creation/administration, native constraint installation,
session-wide auto-merge, overwrite schema replacement, transaction coordination, and native operation metrics are
outside this API. `delta_replace_where` is a same-schema data overwrite, not schema replacement. Native failures
propagate; successful earlier commits remain committed. The new read and selective-overwrite paths remain
release-gated until the pinned live tests run in the ordinary PySpark 4.1.0 / Delta 4.1.0 lane.

See the [Delta API](../api/DeltaTables.api.md) for declarations and examples, the
[Delta background](../background/DeltaTables.back.md) for execution semantics, and the
[V11 Delta specification](../dev/specifications/V11DeltaSchemaBoundMutations.spec.md) for the implementation gate.
