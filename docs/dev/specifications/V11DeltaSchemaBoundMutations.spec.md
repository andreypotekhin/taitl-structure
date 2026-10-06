# V11 Delta Transform Mutations Specification

> Historical V11 contract record. The current durable contract is
> [Delta Tables specification](DeltaTables.spec.md), with user-facing declarations in the
> [Delta tables API](../../api/DeltaTables.api.md).

Public usage and target status: [Delta tables API](../../api/DeltaTables.api.md) and
[Delta compatibility](../../compatibility/DeltaTables.compat.md).

## Status and contract

The following surfaces are implemented and have isolated ordinary PySpark 4.1.0 / Delta 4.1.0 evidence. Public support
remains release-gated until V11's 4.1 capability profile and integration matrix are admitted.

| Surface | Status | Contract |
| --- | --- | --- |
| `Schema.constraints = (check(...),)` | implemented; release-gated | Immutable, symbolic CHECK declarations with stable names. |
| `delta_input(Schema)` | implemented; release-gated | Caller-bound, read-only Delta table relation. |
| `delta_table(Schema)` | implemented; release-gated | Caller-bound read/write relation for same-schema effects. |
| `delta_output(Schema)` | implemented; release-gated | Result schema for an explicit schema transition. |
| `delta_delete`, `delta_update`, `delta_merge` | implemented; release-gated | Compiled, typed table mutations in effect steps. |
| `with_schema_evolution()` on merge/append | implemented; release-gated | Per-operation evolution to the Schema returned by the step. |

## Normative behavior

1. Import and compile paths do not import PySpark or Delta, create sessions, or inspect table data. Invocation binds
   native `delta.tables.DeltaTable` objects under the declared input and output names. An ordinary DataFrame is invalid
   for a Delta binding, and a Delta table is invalid for an ordinary DataFrame input.
2. A same-schema effect step binds a `delta_table` target as a relation parameter, returns `None` or returns its sole
   merge mutation with a matching table-schema annotation, and records compiler-visible Delta operations. Explicit
   schema evolution instead targets a `delta_input` and returns the declared `delta_output` schema as its result. The
   declared Delta output is the caller's original table object; it is not a DataFrame snapshot or command-metric
   relation. Effect steps execute in source order and cannot be pruned as unused.
3. Table shape is validated for every Delta input and output. All declared checks are verified before any mutation,
   unless the effective comparison mode is `off`. Shape validation cannot be disabled by this option. Missing checks
   and mismatches fail before the first mutation; Structure never changes constraint or schema metadata.
4. `delta_check_match` accepts `expression`, `name`, and `off`. The default is `expression`. Resolve method over
   transform over PySpark plugin configuration. `expression` parses both predicates in the supported subset and
   compares their normalized trees, ignoring keyword/function case, insignificant whitespace, and redundant grouping.
   Literal values remain exact and identifier handling follows the active Spark resolver. An unsupported stored
   expression fails closed. `name` requires expected CHECK names, without predicate comparison. `off` skips CHECK
   verification. The effective choice appears in plan/generated metadata without a warning.
5. `delta_delete(target, where=...)` and `delta_update(target, where=..., set=Schema(...))` require an explicit Boolean
   predicate; `where=True` deliberately targets every row. Update assignments must name target fields and be type and
   nullability compatible. A field's physical alias is resolved from the target schema.
6. `delta_merge(target, source, on=...)` requires a Boolean match expression between typed target and source scopes.
   Its symbolic builder admits ordered matched update/delete/update-all, unmatched insert/insert-all, and
   unmatched-by-source update/delete clauses with conditions allowed by Delta 4.1.0. `execute()` captures the complete
   builder once. Source-only fields cannot appear in unmatched-by-source actions. Insert assignments must satisfy the
   target schema. Reject duplicate field assignments, illegal clause order, unsupported strings, and schema evolution
   unless `.with_schema_evolution()` is present and the step returns a distinct declared Delta output schema.
   `delta_append(target, source)` is a typed append; its evolution option is lowered only to that writer's
   `.option("mergeSchema", "true")`.
7. Runtime operations delegate to the native vendor API once, in plan order. They are batch-only and are never retried
   by Structure. A failure keeps its native exception type and gains transform/step context. Earlier successful steps
   may have committed. Structure's internal post-mutation reads use a newly opened native handle. The returned
   original handle can retain a previously materialized `toDF()` snapshot; callers reopen it to inspect the latest commit.
8. The first tested pair is ordinary PySpark 4.1.0 with Delta 4.1.0. Spark Connect is design-gated until
   separately tested. Missing Delta runtimes fail with an actionable diagnostic.

## Acceptance

Spark-free tests cover declarations, compilation, result identity planning, typed predicates and assignments, every
merge clause family, option precedence, and negative diagnostics. Isolated live tests cover preflight failures without
metadata changes, cosmetic expression equivalence, `name` and `off` modes, delete/update/merge effects, explicit
merge/append evolution, native CHECK enforcement, and online/generated parity. `make build`, `make integration`, and `make build INTEGRATION=1` must pass
before the corresponding API rows are marked supported.

## Explicit schema evolution

For a schema transition, the method's `-> OutputSchema` annotation selects a matching `delta_output(OutputSchema)`
declaration. The method takes the current `delta_input(CurrentSchema)` as a relation parameter and returns the sole
mutation result directly:

    def merge(self, change: Change, order: OrderV1) -> OrderV2:
        return (delta_merge(order, change, on=order.id == change.id)
                .with_schema_evolution()
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute())

`delta_merge(...).with_schema_evolution()` enables Delta's native merge schema evolution. The append analog is
`delta_append(order, change).with_schema_evolution().execute()` and applies `mergeSchema` to that append writer only.
Capture rejects a marker unless it is the sole mutation targeting a declared Delta input and its output Schema differs
from the input Schema. Structure validates the current input shape before the operation and the exact declared result
shape and CHECK contract after commit. Session-wide auto-merge, unknown output schemas, metadata installation, and
overwrite schema replacement are excluded.

## Snapshot, CDF, variables, and selective overwrite

1. `variable(type, default=...)` is a per-invocation typed scalar declaration. Support `bool`, `int`, `float`, `str`,
   `bytes`, `Decimal`, `date`, and `datetime`; `None` requires an optional type. Decimal variables declare precision
   and scale. Variable values are validated when an invocation is executed, kept separate from relation inputs and
   compile-time `parameter()` values, and excluded from compiled artifact keys and generated source. A missing required
   variable fails before step execution. Runtime variables may lower to Spark literals and native Delta reader options;
   they cannot control Python branching or graph construction.
2. `delta_snapshot(target, version=...)` and `delta_snapshot(target, timestamp=...)` accept exactly one selector and
   return a relation with the single-output step's annotated result Schema. Timestamp values are timezone-aware and
   converted to the active Spark session timezone.
3. `delta_changes(target, starting_version=..., ending_version=...)` and the corresponding timestamp form use
   inclusive endpoints. A start is required; an end is optional and must match the selector type. The caller must have
   enabled `delta.enableChangeDataFeed=true` before the changes being read were committed. The active Spark session
   must use `io.delta.sql.DeltaSparkSessionExtension` and
   `org.apache.spark.sql.delta.catalog.DeltaCatalog`. Missing preconditions fail before the CDF read starts. CDF
   output schemas declare aliases for `_change_type`, `_commit_version`, and `_commit_timestamp` when those fields are
   needed.
4. Streaming CDF is supplied by the caller as a native `spark.readStream.format("delta")` frame with
   `readChangeFeed=true`, bound to a regular `input(Schema, streaming=True)`. Structure transforms its rows only; the
   caller owns `writeStream`, checkpointing, startup, and shutdown.
5. `delta_replace_where(target, source, where=...).execute()` requires a `delta_output` target, a source with the
   identical Structure Schema, and a Boolean predicate referencing target fields and optional runtime variables only.
   It records one explicit mutation and writes with Delta `replaceWhere`; unsupported predicate expressions fail
   before the commit. Delta enforces that all source rows satisfy the predicate. The operation does not evolve the
   target schema.
6. The snapshot/CDF/replacement additions remain release-gated until pinned live tests pass in ordinary PySpark 4.1.0
   with Delta 4.1.0 for both online and generated execution. Spark Connect is outside the evidence scope.
