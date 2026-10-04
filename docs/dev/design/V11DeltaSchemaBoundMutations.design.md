# V11 Delta Transform Mutations Design

## Purpose

Delta tables participate in Structure's normal compile-and-run workflow. A caller supplies an existing native
`delta.tables.DeltaTable`; Structure compiles typed mutations and verifies the table contract before running them.
Table creation and constraint installation remain caller-owned. The implemented mutation surface rejects implicit
schema changes. A later operation-scoped extension may admit explicit schema evolution while retaining compile-time
and post-commit schema contracts.

## Public shape

`delta_input(Schema)` declares a read-only table relation. `delta_output(Schema)` declares a caller-bound mutable
table target and a named result. The caller supplies native Delta tables as keyword arguments when constructing the
Transform. A successful result exposes the identical table object. A Delta output may be selected as a relation
parameter in an effect step, which returns `None` rather than a row schema.

    class Order(Schema):
        id = string(nullable=False)
        status = string(nullable=False)
        constraints = (check(status != "invalid"),)

    class RemoveRefunds(Transform):
        orders = delta_output(Order)

        @step(input=orders, output=orders)
        def remove(self, order: Order) -> None:
            delta_delete(order, where=order.status == "refunded")

    table = DeltaTable.forPath(spark, path)
    result = RemoveRefunds(orders=table).run(session)
    assert result.orders is table

`delta_update(target, where=..., set=Order(...))` uses a partial target-schema value for assignments.
`delta_merge(target, source, on=...)` has a symbolic, ordered builder covering matched update/delete/update-all,
unmatched insert/insert-all, and unmatched-by-source update/delete. `.execute()` records one operation. Assignment
values and predicates use Structure expressions and physical aliases, not SQL strings. Conditions are optional only
where the corresponding Delta clause permits them. Delete and update require an explicit `where`; `where=True`
denotes an intentional whole-table operation.

## Binding and verification

Each Delta binding validates table shape against the declared `Schema`. `Schema.constraints` carries immutable
`check(predicate, *, name=None)` declarations, with deterministic anonymous names. Every bound Delta input and output
verifies native CHECK metadata before the first mutation. Structure never adds, drops, or replaces a native CHECK.
The default `delta_check_match="expression"` compares the expected and stored predicate as parsed expressions in
the supported CHECK subset. It ignores cosmetic differences but not changed literals or field references. Unparseable
stored expressions fail with a diagnostic. `"name"` requires matching names only; `"off"` skips CHECK verification.
Shape validation remains active in all modes. The option can be set in PySpark plugin configuration, `@transform`, or
`@step`, with the nearest declaration winning. The resolved mode is captured in generated metadata, without a warning.

## Compiler and runtime

The compiler treats Delta steps as effect operations, preserving their order and preventing unused-step pruning.
Compilation neither imports Delta nor opens a Spark session. Runtime binding imports Delta late, checks the native
table type, shape, and constraint metadata, and executes each operation once through the vendor API. Reads of a Delta
relation use a freshly opened native handle after any earlier mutation of that table. The returned object is still
the caller's original handle; Delta 4.1 can keep an earlier `toDF()` snapshot on it if materialized before a mutation.
Callers reopen the table to inspect the latest commit in that case. Native failures retain
their exception type. A later failure does not roll back an earlier committed step, and Structure does not retry an
uncertain commit.

The first tested runtime is ordinary PySpark 4.1.0 with Delta 4.1.0. Public support remains gated on V11's 4.1
capability admission and integration matrix. Spark Connect requires separate evidence.
Full merge *data mutation clause* coverage currently excludes schema evolution, raw SQL, table administration, and
metrics not returned by the native Python API.

## Explicit schema evolution follow-up

Schema evolution is safe enough to expose only when the transform names both the operation and the expected resulting
schema. The merge analog should be `.with_schema_evolution(to=OrderV2)`, lowered to Delta's
`withSchemaEvolution()`. Before execution, the bound table must match either the declaration's current schema or the
named result schema, which permits repeat runs. After the commit, Structure must validate the exact result shape and
its CHECK contract before later steps use it. Compilation must verify that the result is a compatible evolution of the
current schema and that the source relation can supply every added field used by the merge clauses.

Delta's `.option("mergeSchema", "true")` belongs to `DataFrameWriter`, not `DeltaTable`. Its typed analog therefore
needs a distinct append/write effect such as `delta_append(target, source, evolve_to=OrderV2)`. It should resolve the
bound table location at runtime, apply `mergeSchema` only to that one append, and perform the same post-commit contract
check. A session-wide auto-merge configuration is intentionally outside the API because it can evolve unrelated
operations. Overwrite schema replacement is a separate, more destructive contract and should not be implied by this
append option.
