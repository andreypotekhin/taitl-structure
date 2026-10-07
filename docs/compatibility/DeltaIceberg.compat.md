# Delta and Iceberg Compatibility

This page follows the shared compatibility matrix format. Delta and Iceberg integrations are optional and separate
from the ordinary DataFrame API baseline. Live admission is tracked by provider and exact runtime pair. PySpark 4.2,
other Connect profiles, and unlisted provider/runtime combinations are outside these claims. See the
[combined API](../api/DeltaIceberg.api.md) for usage and the separate provider designs and specifications under
`docs/dev/` for developer contracts.

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

## Apache Iceberg

Iceberg helper admission requires live evidence for the exact PySpark and Apache Iceberg runtime pair. A published
upstream pairing alone does not establish Structure support. The admitted helpers currently target format-version 2.
Table cells name the tested Spark and Iceberg pair; `—` means that the profile is not admitted.

| Structure API | PySpark parity | Example | PySpark 3.5 | PySpark 4.0 | PySpark 4.1 | Connect 4.1 | Details |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `iceberg_input(Schema)` | Catalog table read binding | `orders = iceberg_input(Order)` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Read-only except as the source for explicit evolving append. |
| `iceberg_table(Schema)` | Catalog table read/write binding | `orders = iceberg_table(Order)` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Caller supplies the catalog identifier at invocation. |
| `iceberg_output(Schema)` | Evolved schema result binding | `orders = iceberg_output(OrderV2)` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Used for an explicit schema transition; returns the supplied table name. |
| `sql(...)` with Iceberg | Spark SQL queries and commands | `return sql(query, to=Result)` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Tested with configured catalog, extension, typed results, and command/procedure receipts. Other SQL delegates to Spark. |
| `iceberg_append`, `iceberg_update`, `iceberg_delete` | Spark SQL DML / Iceberg writes | `iceberg_update(order, where=..., set=...)` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Typed row mutations; effects are explicit and native commits remain independent. |
| `iceberg_merge` | Iceberg `MERGE INTO` | `iceberg_merge(order, change, on=...)` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Ordered clauses; finish with `.execute()`. |
| `.with_schema_evolution(to=Schema)` | WriterV2 schema merge | `iceberg_append(...).with_schema_evolution(to=V2)` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Additive nullable fields; requires table property `write.spark.accept-any-schema=true`. |
| `iceberg_snapshot` | Snapshot and timestamp reads | `iceberg_snapshot(order, snapshot_id=id)` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Snapshot IDs are opaque Iceberg IDs, not Delta versions. |
| `iceberg_history`, `iceberg_snapshots`, `iceberg_metadata` | Metadata tables and relations | `iceberg_history(order, limit=5)` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Typed projections over native metadata relations. |
| `iceberg_rollback` | `system.rollback_to_snapshot` / timestamp | `iceberg_rollback(order, snapshot_id=id)` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Native ancestry restrictions apply. |
| `iceberg_rewrite_data_files`, `iceberg_rewrite_manifests` | Iceberg rewrite procedures | `iceberg_rewrite_data_files(order).execute()` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Explicit file-layout effects; native metrics remain available through typed SQL. |
| `iceberg_expire_snapshots`, `iceberg_remove_orphan_files` | Iceberg retention procedures | `iceberg_expire_snapshots(order).execute()` | 3.5.3 / 1.12.0 | 4.0.0 / 1.12.0 | 4.1.0 / 1.12.0 | 4.1.0 / 1.12.0 | Retention and file-reference safeguards remain native. |

Live evidence used Iceberg 1.12.0 with Spark 3.5.3 / Scala 2.12, Spark 4.0.0 / Scala 2.13, Spark 4.1.0 / Scala
2.13, and Spark Connect 4.1.0. All 15 native SQL, typed SQL, and helper cases passed in online/generated modes on
each selected lane. The Connect server loaded the Iceberg runtime, extension, and Hadoop catalog. The Spark 3.5 Delta
regression suite passed 31 tests with 3 skips; broader Delta evidence is detailed in the table above.

After rebuilding integration images when runner configuration or Spark pins change, run:

    make integration BACKEND=pyspark35
    make integration BACKEND=pyspark40
    make integration BACKEND=pyspark41
    make integration BACKEND=spark-connect41

The Iceberg table property `write.spark.accept-any-schema=true` and per-write `mergeSchema=true` are required for
evolving append; Structure never sets the table property. Snapshot expiration and orphan removal retain native
retention and reference rules. Changelog helpers, branch/tag administration, and non-v2 formats are not admitted.

The Spark-free `make build` quality gates passed formatting, lint, and mypy. Its full pytest step reported 2,322
passed, 322 skipped, and four existing Structured Streaming and Transformation coverage-ledger failures outside these
provider integrations. `poetry build` succeeded. SQL delegates to Spark and Iceberg; untested SQL behavior does not
imply admission of a corresponding Structure helper.
