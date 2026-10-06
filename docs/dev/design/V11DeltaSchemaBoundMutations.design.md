# V11 Delta Transform Mutations Design

> Historical V11 design record. The current API uses `delta_table(Schema)` for the common same-schema read/write
> binding; see the durable [Delta Tables design](DeltaTables.design.md), [specification](../specifications/DeltaTables.spec.md),
> and [public reference](../../reference/DeltaTables.ref.md).

Public usage and target status: [Delta tables API](../../api/DeltaTables.api.md) and
[Delta compatibility](../../compatibility/DeltaTables.compat.md).

## Purpose

Delta tables participate in Structure's normal compile-and-run workflow. A caller supplies an existing native
`delta.tables.DeltaTable`; Structure compiles typed mutations and verifies the table contract before running them.
Table creation and constraint installation remain caller-owned. Mutations preserve the bound schema by default.
Schema evolution is allowed only when a mutation explicitly opts in and names its expected output schema. Structure
verifies the declared old schema before the operation and the selected new schema after the commit.

## Public shape

`delta_table(Schema)` declares the common caller-bound read/write relation. `delta_input(Schema)` is read-only and
`delta_output(Schema)` declares a result schema for explicit evolution. The caller supplies native Delta tables as
keyword arguments when constructing the Transform. A successful result exposes the identical table object. Ordinary
same-schema effects may use `-> None`, with inference when relation resolution is unambiguous, or return the direct
result of a merge with a matching table-schema annotation. An explicit schema transition can return the declared
`delta_output` schema, which lets normal return-schema resolution select the result without extra step parameters or
decorator metadata. The required `to=Schema` argument independently selects the physical evolved shape; it must match
`delta_output` when one is declared. A `delta_table` effect may use `-> None` and omit a return, retaining its relation
schema without exposing a separate composable output.

    class Order(Schema):
        id = string(nullable=False)
        status = string(nullable=False)
        constraints = (check(status != "invalid"),)

    class RemoveRefunds(Transform):
        orders = delta_table(Order)

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
Full merge *data mutation clause* coverage includes opt-in schema evolution. Raw SQL, table administration, and
metrics not returned by the native Python API remain outside this surface.

## Explicit schema evolution

Schema evolution is opt-in through a return-typed state transition:

    changes = input(Change)
    current_orders = delta_input(OrderV1)
    orders = delta_output(OrderV2)

    def merge(self, change: Change, order: OrderV1) -> OrderV2:
        return (delta_merge(order, change, on=order.id == change.id)
                .with_schema_evolution(to=OrderV2)
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute())

The return schema resolves to the declared Delta output. Capture accepts the marker only when it represents the
step's sole mutation, its target is the declared `delta_input`, and `to` matches the output schema. The table must match
`OrderV1` before mutation; after commit Structure validates `OrderV2`, including its declared CHECK contract. A later
invocation must declare the table's current schema as its input rather than silently accepting either version.

Merge evolution lowers to the native builder's `withSchemaEvolution()`. Append evolution is a separate typed
`delta_append(target, source).with_schema_evolution(to=Schema).execute()` operation because Delta's `.option("mergeSchema",
"true")` belongs to `DataFrameWriter`, not `DeltaTable`. It resolves the bound table location at runtime, applies the
option only to that append, and performs the same post-commit check. Session-wide auto-merge is excluded because it
could evolve unrelated operations. Overwrite schema replacement is a separate, more destructive contract and is not
implied by this append option.

## Historical reads and runtime selectors

Add `variable(type, default=...)` as a core transform declaration distinct from compile-time `parameter()`. Its
symbolic access produces a typed expression reference; its value travels through an invocation-only channel for
online and generated execution. Variable bindings do not enter artifact keys or generated source. They can participate
in Spark expressions and driver-side Delta reader options, but cannot select Python branches or change the graph.

`delta_snapshot` and `delta_changes` are relation-producing transform operations. A single-output step's return
annotation chooses the output Schema, including aliases for CDF's `_change_type`, `_commit_version`, and
`_commit_timestamp` columns. Reads use the caller's native table handle and never change its schema or metadata.
Snapshot selection is by exactly one version or aware timestamp. CDF uses one start selector and an optional end of
the same kind; endpoints are inclusive. The table must already have `delta.enableChangeDataFeed=true`, and the Spark
session must have the Delta extension and catalog. Structure checks those preconditions before the CDF reader opens.
Delta history retention and enabling time still constrain available events.

The streaming path remains a caller-created `readStream` DataFrame bound through a regular streaming input. Structure
transforms the rows; the caller owns stream startup, sink, checkpoint, and shutdown. `delta_replace_where` is a typed,
same-schema mutation builder whose `.execute()` records a native `replaceWhere` overwrite. Its predicate is a restricted
typed SQL subset, permits target fields and invocation variables, and is safely rendered as a Delta SQL option; Delta
performs its native source-row predicate check.
