# Delta Tables Reference

Structure's Delta API compiles typed table effects and reads into transforms. The caller creates/configures the Delta
table and passes its native `DeltaTable` handle to the transform.

| API | Use |
| --- | --- |
| `delta_table(Schema)` | Same-schema caller-bound relation for reads and mutations. |
| `delta_input(Schema)` | Read-only caller-bound relation; also used as an evolution source. |
| `delta_output(Schema)` | Declared result schema for explicit schema evolution. |
| `delta_delete`, `delta_update`, `delta_merge`, `delta_append` | Typed mutation builders. |
| `delta_replace_where(...).execute()` | Same-schema selective overwrite. |
| `delta_snapshot`, `delta_changes` | Historical snapshot and batch CDF reads. |
| `check(predicate, name=...)` | Expected native Delta CHECK metadata in `Schema.constraints`. |

For same-schema mutations, typed `-> None` methods infer a unique target; `@step` disambiguates when needed. A merge may
also return `.execute()` directly with a return annotation equal to the table schema. For schema evolution, use
`delta_input(Current)` plus `delta_output(New)`, call `.with_schema_evolution()`, and return the operation with the
`New` annotation. See the [API examples](../api/DeltaTables.api.md), [mutation recipe](../recipes/DeltaTableMutations.md),
[evolution recipe](../recipes/DeltaSchemaEvolution.md), and [CDF recipe](../recipes/DeltaChangeDataFeed.md).
