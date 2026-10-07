# Delta and Iceberg Tables API

Structure provides typed operations for caller-owned Delta and Apache Iceberg tables. These optional provider
integrations are admitted only for the exact runtime profiles in the
[combined compatibility ledger](../compatibility/DeltaIceberg.compat.md).

Import table declarations and operations from `structure.plugin.pyspark` alongside `Schema` and `Transform` from
`structure`.

## Delta tables

| API | Purpose |
| --- | --- |
| `delta_input(Schema)`, `delta_table(Schema)`, `delta_output(Schema)` | Bind native `DeltaTable` handles for reads, same-schema effects, and schema transitions. |
| `delta_append`, `delta_delete`, `delta_update`, `delta_merge`, `delta_replace_where` | Typed row mutations. |
| `delta_snapshot`, `delta_changes` | Historical and change-feed reads. |
| `delta_history`, `delta_detail` | Typed metadata reads. |
| `delta_restore`, `delta_optimize`, `delta_vacuum` | Explicit maintenance effects. |
| `check`, `delta_generated`, `delta_identity`, `delta_default` | Declare expectations for existing table constraints and column features. |

See the [Delta reference](../reference/DeltaIceberg.ref.md#delta-tables) for signatures, constraints, and examples, or
the [Delta background](../background/DeltaIceberg.back.md#delta-tables) for effect and lifecycle semantics.

## Apache Iceberg

| API | Purpose |
| --- | --- |
| `iceberg_input(Schema)`, `iceberg_table(Schema)`, `iceberg_output(Schema)` | Bind catalog identifiers for reads, same-schema effects, and evolving append results. |
| `iceberg_append`, `iceberg_update`, `iceberg_delete`, `iceberg_merge` | Typed row mutations. |
| `iceberg_snapshot`, `iceberg_history`, `iceberg_snapshots`, `iceberg_metadata` | Historical and metadata reads. |
| `iceberg_rollback`, `iceberg_rewrite_data_files`, `iceberg_rewrite_manifests` | Rollback and file-layout effects. |
| `iceberg_expire_snapshots`, `iceberg_remove_orphan_files` | Explicit retention and cleanup effects. |

Iceberg SQL and procedures remain available through the typed `sql(...)` helper. Helpers are convenience operations
that preserve native Iceberg behavior; schema evolution is append-only and requires the caller's table property
`write.spark.accept-any-schema=true`.

See the [Iceberg reference](../reference/DeltaIceberg.ref.md#apache-iceberg-tables) for authoring examples and operation
contracts, and the [background](../background/DeltaIceberg.back.md#apache-iceberg-tables) for catalogs, SQL, and
lifecycle semantics. The [compatibility ledger](../compatibility/DeltaIceberg.compat.md#apache-iceberg) lists the
admitted runtime pairs and tested scope.
