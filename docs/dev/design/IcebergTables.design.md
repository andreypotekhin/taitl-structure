# Iceberg table transforms

## Purpose

Iceberg tables use Spark SQL and DataSource V2 writes through a configured Spark catalog. Structure should make
common table actions available inside typed transforms while preserving direct SQL for all supported Iceberg
statements and procedures. The caller configures the Spark session and catalog, creates tables, and binds table names
to transform declarations.

## Design

The first integration surface is the existing typed `sql(...)` operation. It accepts SQL text, typed DataFrame
relation scopes or caller-owned SQL relation strings, literal arguments, and a declared output schema. It does not
parse Iceberg SQL. Native procedure output schemas remain user-declared Structure schemas.

The convenience layer uses `iceberg_input(Schema)`, `iceberg_table(Schema)`, and `iceberg_output(Schema)` and binds
caller-supplied catalog table identifiers at invocation time. Keep the identifier as runtime data in compiled plans;
do not compile a concrete table name into an artifact. A bound table is distinct from an ordinary DataFrame output.
Reads use Spark catalog APIs. Mutation helpers lower typed predicates, assignments, and merge clauses to native SQL
or a native Spark writer operation. Use public Spark APIs so the same design can work with classic Spark and Connect.

Keep provider-specific symbolic payloads under `src/structure/plugin/pyspark/iceberg/`. The core frontend must
recognize these binding kinds and preserve the caller's table identity through input discovery, effect resolution,
generated-code rendering, and output access. Capability checks are operation-specific and apply before execution.
Avoid changes to generic plugin contracts unless the Iceberg feature cannot fit the PySpark plugin model.

`iceberg_append(...).with_schema_evolution(to=NewSchema).execute()` is the only helper-level evolution form. It uses
DataFrameWriterV2 with the `mergeSchema=true` write option. The table must already carry
`write.spark.accept-any-schema=true`; Structure never changes that property. Initially allow additive nullable columns
and reject inferred rename, drop, and type changes. SQL remains available for other schema changes.

Maintenance follows Iceberg terms: snapshot rollback, data-file rewrite, manifest rewrite, snapshot expiration, and
orphan-file removal. Keep these explicit and separate. Do not treat expiration as Delta restore or a change feed, do
not combine cleanup operations, and retain native procedure defaults and safeguards. Detailed procedure metrics stay
available as ordinary typed SQL rows.

## Runtime policy

Use Apache Iceberg 1.12.0 artifacts in isolated tests for PySpark 3.5, 4.0, and 4.1, plus Spark Connect 4.1. The
planned helper evidence uses format-version 2. Pin Scala suffixes to the Spark distribution. The test runner and Spark
Connect server install the matching runtime and Iceberg SQL extension before session creation. Connect configuration
belongs on the server. Keep Iceberg sessions separate from other integration sessions where they require different
extensions or catalogs.

The append-only incremental reader is not a change-data-feed equivalent. Changelog helpers, catalog provisioning,
branch/tag administration, and streaming writes remain outside this integration. Users can use supported provider SQL
through `sql(...)` without Structure implementing those surfaces.

## Failure behavior

Preserve Spark and Iceberg exception types. Add Structure step context at immediate execution/action boundaries where
available. Do not retry commits. A transport error or output-schema validation error can follow a successful commit;
surface that possibility. Require explicit opt-in for schema merge and explicit invocation for maintenance. Keep
offline compilation free of Spark calls and optional Iceberg imports.

## Validation

Compare native SQL, online transforms, and generated transforms on isolated tables in every admitted runtime. Cover
binding validation, mutations, historical reads, evolving append, typed procedure output, cleanup, identifier quoting,
and failures. Require `make build` and the selected live integration matrix before updating compatibility status.
