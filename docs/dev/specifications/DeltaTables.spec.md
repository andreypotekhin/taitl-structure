# Delta Tables Specification

## Scope

This specification defines typed transform bindings for caller-owned Delta tables, their mutation effects, checks, and
read operations. Table creation, constraint installation, arbitrary SQL, and general Delta administration remain
caller-owned. Current support is release-gated as described in the [compatibility ledger](../../compatibility/DeltaTables.compat.md).

## Declarations

1. `delta_table(Schema)` declares one caller-bound Delta relation that may be read and mutated while preserving its
   schema. It is eligible for normal typed relation resolution and may appear in `@step(input=..., inout=...)`.
2. `delta_input(Schema)` declares a caller-bound relation used for snapshot/CDF reads and as the source of row-write
   schema evolution. Ordinary row mutations fail on this binding. `delta_restore` may use it as the current table when
   the typed return resolves to a distinct `delta_output` post-restore schema.
3. `delta_output(Schema)` declares the result schema for a schema-evolving mutation. It is not a caller-bound input and
   cannot be used as a mutation target.
4. The caller must bind each required Delta declaration to a native `DeltaTable`. A DataFrame is not a valid Delta
   binding. Successful mutation results retain the caller's table object identity.
5. `Schema.delta_columns` may contain `delta_generated(field, as_=sql)`, `delta_identity(field, mode, start, step)`,
   and `delta_default(field, value)` declarations. They describe existing native table metadata; they do not create or
   alter Delta features. A declaration must reference a field of the declaring Schema. Identity fields use Structure
   `long`.

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

1. Without `.with_schema_evolution(to=...)`, row mutations must preserve the bound physical schema. The compiler checks typed
   assignments and result binding compatibility. `delta_restore` is a separate maintenance effect and validates its
   declared post-restore schema after the native commit.
2. With `.with_schema_evolution(to=Schema)`, the required `to` schema is the mutation's physical output schema. Each
   pre-existing target field must remain represented by a compatible output
   field. Types must be compatible; a non-nullable target cannot become nullable. Every new output field must be
   supplied by the source projection or explicit assignment. `*_all` clauses and schema-evolving append must supply
   every output field from the source. Insert paths must supply every non-nullable output field.
3. For an explicit `delta_output`, `to` must match its declared schema. A `delta_table` effect may evolve without
   declaring a separate output relation; the transform retains its bound relation schema and does not expose the
   evolved table as a composable transform output. Before mutation, runtime checks the Delta table's physical columns,
   types, nullability, and declared native CHECK
   metadata. After an evolving commit it checks the output schema and output CHECK metadata. A post-commit validation
   failure does not roll back the already committed Delta operation.
4. `Schema.constraints` declares expected native `CHECK` constraints. Structure validates them but never creates,
   changes, or drops Delta constraints. `delta_check_match` supports `expression`, `name`, and `off`; `off` skips CHECK
   comparison only and never disables schema validation.
5. At binding, generated expressions and identity mode/start/step are compared with Delta log field metadata; defaults
   are compared with the native `CURRENT_DEFAULT`. A mismatch fails before a write. On insert, only declared generated,
   identity, and default fields may be omitted from non-nullable assignments. A `GENERATED ALWAYS` identity field may
   not be assigned. Explicit generated values are left to Delta's native equality check.

## Supported operations and reads

- `delta_delete` and `delta_update` use typed Boolean predicates; update values are typed Structure projections.
- `delta_merge` records supported ordered matched, unmatched, and unmatched-by-source clauses.
- `delta_append` appends a typed relation; schema evolution is scoped to its writer.
- `delta_replace_where(...).execute()` performs a typed selective overwrite and requires the target's schema.
- `delta_snapshot` and `delta_changes` are read operations over `delta_input` relations. Their step return annotation
  declares an ordinary DataFrame output schema.
- `delta_history` and `delta_detail` are typed metadata reads over `delta_input` or `delta_table`; result annotations
  declare the subset of history/detail fields to expose. `delta_history(limit=...)` accepts `None` or a positive
  integer, including a `variable(int)`.
- `delta_restore(...).execute()` creates a restore commit and returns the original table relation. It accepts exactly
  one version or timestamp selector. The result annotation and binding validate the declared post-restore schema; native
  restore metrics are not an output in this contract.
- `delta_optimize(...).execute_compaction()` and `.execute_zorder(by=(...))` are explicit file-layout effects. A
  `where=` predicate is validated against native partition columns. Native metric DataFrames are not exposed.
- `delta_vacuum(...).execute()` defaults to 168 hours. A shorter retention requires `allow_short_retention=True` and
  remains subject to Delta's native retention safety check. Vacuum deletes unreferenced files and may make old snapshots
  unreadable.
- By default, CDF preflight checks require `delta.enableChangeDataFeed=true` on the table and case-insensitive `delta`
  in both session extension and catalog values. `delta_cdf_checks=False` disables both checks; it does not enable CDF
  or alter Spark/Delta behavior. The caller enables the table property and configures the session.

## Execution guarantees and limits

Compilation does not import PySpark/Delta, open a session, or access table data. Runtime mutation failures retain native
exception types. Structure does not retry uncertain commits or provide multi-step transactions. Streaming CDF ingestion
uses a caller-created streaming DataFrame; the caller owns query startup, checkpointing, sink, and shutdown.
