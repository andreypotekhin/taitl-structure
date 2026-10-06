# Delta Tables Design

## Purpose

Delta tables are persistent state, so their mutations need to be visible to Structure's normal compile-and-run workflow.
The caller owns table creation and supplies a native `delta.tables.DeltaTable`; Structure resolves typed relations,
checks the declared schema and constraints, and executes explicit mutation effects.

## Bindings and method shape

`delta_table(Schema)` is the common read/write binding for a table mutated without changing its schema. It participates
in relation resolution like an ordinary input. A typed `-> None` method can infer the participating table when the
declared relation is unambiguous; `@step` declarations disambiguate when needed. An author may also return
`delta_merge(...).execute()` and annotate that method with the same `Schema`. This keeps the result visible in Python's
type signature while compile-time validation confirms that the operation targets a bound table with that schema.

`delta_input(Schema)` is read-only and serves snapshot/CDF reads and schema-evolution sources.
`delta_output(Schema)` names the expected result schema of an explicitly opted-in schema transition. Keeping these
declarations distinct prevents an output declaration from silently becoming a mutable input.

## Schema safety

The default mutation contract preserves the declared schema. Structure validates physical names, types, nullability,
and declared native CHECK constraints before mutation. CHECK constraints are authored as `Schema.constraints`, but the
caller provisions the corresponding Delta metadata. An explicit `.with_schema_evolution()` is the only mutation path
that permits schema change. Its return annotation selects the output declaration, and static checks verify that the
target, source, and result schemas are compatible before generated code runs. Runtime validation checks the new table
shape and constraints after the native commit.

Schema-evolving merge uses Delta's `withSchemaEvolution()` builder operation. Schema-evolving append sets
`mergeSchema=true` on that individual writer. Structure does not enable session-wide auto-merge because that would
affect unrelated operations.

## Effects and execution

Delta mutations are compiler-visible effects and cannot be pruned as unused. Calls execute in source order, once per
step. `delta_delete`, `delta_update`, merge, append, and `delta_replace_where` retain their native Delta commit
semantics. They do not provide a cross-step transaction or rollback. The returned transform result retains the caller's
table handle; callers should reopen/read the table after a commit if they previously materialized a DataFrame snapshot.

Compilation is Spark-free. Runtime binding verifies that each declaration received a native Delta table and checks
Spark settings required by operations such as CDF. The current release remains gated to the compatibility ledger's
tested profile.

## Related contracts

- Normative behavior: [Delta tables specification](../specifications/DeltaTables.spec.md)
- User API and examples: [Delta tables API](../../api/DeltaTables.api.md)
- Vendor mapping and release status: [Delta tables compatibility](../../compatibility/DeltaTables.compat.md)
- Historical V11 design record: [V11 Delta mutations](V11DeltaSchemaBoundMutations.design.md)
