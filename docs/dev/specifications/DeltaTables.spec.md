# Delta Tables Specification

## Scope

This specification defines typed transform bindings for caller-owned Delta tables, their mutation effects, checks, and
read operations. Table creation, constraint installation, arbitrary SQL, and general Delta administration remain
caller-owned. Current support is release-gated as described in the [compatibility ledger](../../compatibility/DeltaTables.compat.md).

## Declarations

1. `delta_table(Schema)` declares one caller-bound Delta relation that may be read and mutated while preserving its
   schema. It is eligible for normal typed relation resolution and may appear in `@step(input=..., inout=...)`.
2. `delta_input(Schema)` declares a caller-bound read-only relation. Mutating it fails compilation/runtime capture. It
   may be used by snapshot and CDF reads and as the source table for explicit schema evolution.
3. `delta_output(Schema)` declares the result schema for a schema-evolving mutation. It is not a caller-bound input and
   cannot be used as a mutation target.
4. The caller must bind each required Delta declaration to a native `DeltaTable`. A DataFrame is not a valid Delta
   binding. Successful mutation results retain the caller's table object identity.

## Step effects and typing

1. A same-schema mutation method may return `None`. When typed relation resolution identifies a unique `delta_table`
   target, the compiler infers that table as the mutation effect. Existing `@step` input/output/inout declarations
   disambiguate relations when inference is ambiguous.
2. A method may instead return the result of its sole `delta_merge(...).execute()` call and annotate the method with
   the target schema. Compilation requires the selected result schema to be identical to the bound target declaration
   and rejects incompatible annotations.
3. A schema-evolving mutation must return the result of its sole merge or append `.execute()` call. The return
   annotation resolves to a distinct `delta_output` schema, and the mutation must target a `delta_input` declaration.
4. Mutation methods may contain Delta mutation operations only. Their operations execute in source order and remain
   effects even if their return value is not otherwise consumed. `delta_replace_where` requires `.execute()`.

## Static and runtime schema checks

1. Without `.with_schema_evolution()`, mutation must preserve the bound physical schema. The compiler checks typed
   assignments and result binding compatibility.
2. With `.with_schema_evolution()`, each pre-existing target field must remain represented by a compatible output
   field. Types must be compatible; a non-nullable target cannot become nullable. Every new output field must be
   supplied by the source projection or explicit assignment. `*_all` clauses and schema-evolving append must supply
   every output field from the source. Insert paths must supply every non-nullable output field.
3. Before mutation, runtime checks the Delta table's physical columns, types, nullability, and declared native CHECK
   metadata. After an evolving commit it checks the output schema and output CHECK metadata. A post-commit validation
   failure does not roll back the already committed Delta operation.
4. `Schema.constraints` declares expected native `CHECK` constraints. Structure validates them but never creates,
   changes, or drops Delta constraints. `delta_check_match` supports `expression`, `name`, and `off`; `off` skips CHECK
   comparison only and never disables schema validation.

## Supported operations and reads

- `delta_delete` and `delta_update` use typed Boolean predicates; update values are typed Structure projections.
- `delta_merge` records supported ordered matched, unmatched, and unmatched-by-source clauses.
- `delta_append` appends a typed relation; schema evolution is scoped to its writer.
- `delta_replace_where(...).execute()` performs a typed selective overwrite and requires the target's schema.
- `delta_snapshot` and `delta_changes` are read operations over `delta_input` relations. Their step return annotation
  declares an ordinary DataFrame output schema.
- CDF requires `delta.enableChangeDataFeed=true` on the table and the Delta Spark extension/catalog settings on the
  session. The caller enables the table property and configures the session.

## Execution guarantees and limits

Compilation does not import PySpark/Delta, open a session, or access table data. Runtime mutation failures retain native
exception types. Structure does not retry uncertain commits or provide multi-step transactions. Streaming CDF ingestion
uses a caller-created streaming DataFrame; the caller owns query startup, checkpointing, sink, and shutdown.
