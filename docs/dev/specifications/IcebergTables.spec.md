# Iceberg Tables Specification

## 1. Scope

1. This feature integrates Apache Iceberg tables through Spark SQL in the PySpark plugin.
2. The caller configures Spark, installs the matching Iceberg runtime and SQL extensions, creates tables, and supplies
   the catalog identifier at transform invocation.
3. Structure provides generic typed SQL and a set of `iceberg_` declarations and helpers for common batch table work.
4. Structure does not create catalogs, provision tables, manage credentials, retry commits, or promise transactions
   across several transform steps.
5. The initial helper contract targets format-version 2 only and the profiles listed in the compatibility ledger.

## 2. SQL

1. Iceberg DDL, DML, time travel, metadata queries, and supported stored procedures are expressed through the existing
   `sql(statement, ..., to=Schema)` helper.
2. Structure does not parse provider SQL or infer its result schema. Query and tabular procedure rows require a
   declared `Schema`; a normalized command may use `SqlCommandResult`.
3. A tabular procedure's typed rows retain all columns supplied by Spark and must not be normalized as generic
   affected-row counts.
4. An effectful SQL statement executes once per transform invocation when its operation is reached, even when its
   result is not returned. Collecting an already returned result does not run the statement again.
5. Mixed SQL/helper effects execute in recipe order. SQL effects retain native exceptions, with Structure step
   context appended when execution occurs within Structure's control.

## 3. Table declarations

1. `iceberg_input(Schema)` declares a caller-bound read-only table. It may be a source for append-time schema
   evolution.
2. `iceberg_table(Schema)` declares a caller-bound table that can be read and mutated without a schema change.
3. `iceberg_output(Schema)` declares a schema transition result. It is not a transform invocation input.
4. Bound values are catalog identifiers represented as Python strings. They are runtime values and are not captured
   into compiled artifacts.
5. The runtime verifies the binding is an Iceberg table and validates its schema against the declaration. It uses
   public Spark SQL/catalog APIs and does not require a client JVM or PyIceberg.
6. A table binding and table result preserve the identifier string. A normal `output(Schema)` remains a DataFrame.
7. A transform child retains an inherited Iceberg declaration's provider, role, and exact schema. Overridden methods
   replace inherited methods in place, while `super()` schedules the parent table step immediately before the child.
8. Composition can pass a same-schema Iceberg table result to an `iceberg_input` or `iceberg_table` consumer. Stage
   names and output aliases do not change the caller's catalog identifier. A DataFrame, Delta table, or read-only
   wrapper input cannot be adapted or escalated into a mutable Iceberg table.

## 4. Row operations

1. `iceberg_update(target, *, where, set)` and `iceberg_delete(target, *, where)` are explicit typed effects.
2. `where` is a Boolean Structure expression. `where=True` intentionally applies to all rows.
3. `set` is an instance of the target schema with one or more populated fields. Assignment expressions may refer only
   to allowed target/source scopes for the operation.
4. `iceberg_append(target, source).execute()` appends source rows aligned by physical column name.
5. `iceberg_merge(target, source, *, on)` builds ordered matched and not-matched update/delete/insert clauses and
   commits when `.execute()` is called. Structure preserves native duplicate-match failures and does not deduplicate.
6. An ordinary mutation may not change the declared table schema. Schema-merge opt-in is available only on append.

## 5. Reads and inspection

1. Current table reads use the caller-bound relation and declared schema.
2. `iceberg_snapshot(target, *, snapshot_id=None, timestamp=None)` requires exactly one selector and returns the
   schema of the selected historical table state.
3. Snapshot IDs are Iceberg identifiers and are not described as Delta versions. Timestamps are timezone-aware and
   interpreted consistently with Spark's session timezone setting.
4. `iceberg_history(target, *, limit=None)`, `iceberg_snapshots(target, *, limit=None)`, and
   `iceberg_metadata(target, *, kind, to)` return typed relations from the corresponding native metadata tables.
5. Metadata kind is a compile-time choice. The result schema names a supported subset of native metadata columns.
6. Table reads after an explicit helper mutation observe the new table state. Historical frames keep their selected
   snapshot. Generic SQL is opaque; later reads refresh table state after SQL execution.

## 6. Schema evolution

1. `iceberg_append(target, source).with_schema_evolution(to=NewSchema).execute()` declares an additive append
   transition from `iceberg_input(OldSchema)` to `iceberg_output(NewSchema)`.
2. `to` must match the declared transition result and differ from the current schema.
3. Initially, new columns must be nullable and the old columns must retain their physical names and types. Renames,
   drops, type changes, and merge-time schema changes are rejected by helpers.
4. The table must already set `write.spark.accept-any-schema=true`. The write uses Iceberg DataFrameWriterV2 with
   `mergeSchema=true` for that append. Structure does not alter catalog or table configuration.
5. Validate compatibility before writing and validate the resulting table schema after commit. If post-commit
   validation fails, the error states that the native commit may already have succeeded.
6. Other schema changes remain available through native SQL.

## 7. Snapshot rollback and maintenance

1. `iceberg_rollback(target, *, snapshot_id=None, timestamp=None).execute()` requires exactly one selector and uses
   Iceberg's native rollback procedure. Native snapshot ancestry rules apply.
2. `iceberg_rewrite_data_files`, `iceberg_rewrite_manifests`, `iceberg_expire_snapshots`, and
   `iceberg_remove_orphan_files` are distinct explicit operations completed with `.execute()`.
3. Procedure-specific argument values keep native meanings and omission uses the native default. Structure does not
   invent Delta retention defaults, combine cleanup actions, or weaken native safeguards.
4. Users who need procedure result metrics can issue `CALL` through `sql(..., to=ResultSchema)`.

## 8. Compatibility and failure

1. Helper calls require an admitted PySpark profile and operation capability. Initially test PySpark 3.5, 4.0, and
   4.1 classic plus Spark Connect 4.1 with Iceberg 1.12.0, using a matching Scala runtime artifact.
2. Connect installs extensions, catalogs, and jars server-side. Runtime validation must not use `_jvm`, `_jdf`, or
   other classic-only access.
3. Unknown features, streaming mutation use, unsupported expression forms, mismatched schemas, and non-Iceberg table
   bindings fail before commit when Structure can determine the condition.
4. A commit failure remains a native error. A network error or later validation failure may leave commit status
   unknown. Structure does not retry.
5. Composed table consumers validate and read the table after prior steps commit. Effects in separate steps commit
   separately; a later failure leaves earlier commits in place. An explicit snapshot remains historical.
5. Changelog helpers, incremental read helpers, branch/tag management, and table provisioning are outside this API.
   Incremental reads exposed by Spark/Iceberg do not include all mutation kinds and are not represented as a change
   feed. Supported upstream SQL remains available through `sql(...)`.
