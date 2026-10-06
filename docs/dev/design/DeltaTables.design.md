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

`delta_input(Schema)` does not accept row mutations and serves snapshot/CDF reads and schema-evolution sources. A
restore may target it when the step return annotation selects the distinct `delta_output` post-restore schema.
`delta_output(Schema)` names the expected result schema of an explicitly opted-in schema transition. Keeping these
declarations distinct prevents an output declaration from silently becoming a mutable input.

## Schema safety

The default mutation contract preserves the declared schema. Structure validates physical names, types, nullability,
and declared native CHECK constraints before mutation. CHECK constraints are authored as `Schema.constraints`, but the
caller provisions the corresponding Delta metadata. An explicit `.with_schema_evolution(to=Schema)` is the row-write
path that permits schema change. The `to` schema drives static compatibility checks. With `delta_input` and
`delta_output`, it must match the declared output; `delta_table` can instead record an evolved effect without exposing
a separate composable output. Runtime validation checks the new table shape and constraints after the native commit.

Schema-evolving merge uses Delta's `withSchemaEvolution()` builder operation. Schema-evolving append sets
`mergeSchema=true` on that individual writer. Structure does not enable session-wide auto-merge because that would
affect unrelated operations.

Delta column behavior is an explicit schema contract in `Schema.delta_columns`. `delta_generated(field, as_=...)`,
`delta_identity(field, ...)`, and `delta_default(field, value=...)` declare expectations about a table the caller has
already created. Binding checks those expectations against the Delta log schema (and Spark's current default metadata).
Only declared generated, identity, and default columns may be omitted from insert values. `GENERATED ALWAYS` identity
fields cannot be assigned; Delta remains responsible for checking explicitly supplied generated-column values.

## Effects and execution

Delta mutations are compiler-visible effects and cannot be pruned as unused. Calls execute in source order, once per
step. `delta_delete`, `delta_update`, merge, append, and `delta_replace_where` retain their native Delta commit
semantics. They do not provide a cross-step transaction or rollback. The returned transform result retains the caller's
table handle; callers should reopen/read the table after a commit if they previously materialized a DataFrame snapshot.

Compilation is Spark-free. Runtime binding verifies that each declaration received a native Delta table and checks
Spark settings required by operations such as CDF. CDF preflight checks default to requiring the enabled table
property and case-insensitive `delta` substrings in the session extension and catalog values. The plugin,
`@transform`, or `@step` option `delta_cdf_checks=False` disables both CDF preflight checks without changing the read.
The current release remains gated to the compatibility ledger's tested profile.

History and detail use the same typed relation-result path as snapshots and CDF. Restore, optimize, and vacuum are
explicit effect steps: a restore return annotation declares the restored table shape, optimize rewrites files, and
vacuum deletes unreferenced files subject to Delta's retention guard. Their native metric DataFrames are not exposed
as transform outputs in this contract. A vacuum retention under 168 hours requires an explicit source opt-in and still respects Delta's native
safety setting.

## Related contracts

- Normative behavior: [Delta tables specification](../specifications/DeltaTables.spec.md)
- User API and examples: [Delta tables API](../../api/DeltaTables.api.md)
- Vendor mapping and release status: [Delta tables compatibility](../../compatibility/DeltaTables.compat.md)
- Historical V11 design record: [V11 Delta mutations](V11DeltaSchemaBoundMutations.design.md)
- Remaining API families and order: [Delta parity queue](../deferred/DeltaTables.deferred.md)
