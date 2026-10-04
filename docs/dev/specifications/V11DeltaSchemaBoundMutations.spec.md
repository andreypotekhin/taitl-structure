# V11 Delta Transform Mutations Specification

## Status and contract

The following surfaces are implemented and have isolated ordinary PySpark 4.1.0 / Delta 4.1.0 evidence. Public support
remains release-gated until V11's 4.1 capability profile and integration matrix are admitted.

| Surface | Status | Contract |
| --- | --- | --- |
| `Schema.constraints = (check(...),)` | implemented; release-gated | Immutable, symbolic CHECK declarations with stable names. |
| `delta_input(Schema)` | implemented; release-gated | Caller-bound, read-only Delta table relation. |
| `delta_output(Schema)` | implemented; release-gated | Caller-bound mutable target returned by identity after success. |
| `delta_delete`, `delta_update`, `delta_merge` | implemented; release-gated | Compiled, typed table mutations in effect steps. |

## Normative behavior

1. Import and compile paths do not import PySpark or Delta, create sessions, or inspect table data. Invocation binds
   native `delta.tables.DeltaTable` objects under the declared input and output names. An ordinary DataFrame is invalid
   for a Delta binding, and a Delta table is invalid for an ordinary DataFrame input.
2. An effect step binds its target as a relation parameter, returns `None`, and records one or more compiler-visible
   Delta operations. Only a `delta_output` parameter may be mutated. The declared output is the caller's original
   table object; it is not a DataFrame snapshot or a command-metric relation. Effect steps execute in source step order
   and cannot be pruned as unused.
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
   target schema. Reject duplicate field assignments, illegal clause order, unsupported strings, and implicit schema
   evolution during compilation. Explicit operation-scoped schema evolution is a designed follow-up and is not part of
   this implemented surface.
7. Runtime operations delegate to the native vendor API once, in plan order. They are batch-only and are never retried
   by Structure. A failure keeps its native exception type and gains transform/step context. Earlier successful steps
   may have committed. Structure's internal post-mutation reads use a newly opened native handle. The returned
   original handle can retain a previously materialized `toDF()` snapshot; callers reopen it to inspect the latest commit.
8. The first tested pair is ordinary PySpark 4.1.0 with Delta 4.1.0. Spark Connect is design-gated until
   separately tested. Missing Delta runtimes fail with an actionable diagnostic.

## Acceptance

Spark-free tests cover declarations, compilation, result identity planning, typed predicates and assignments, every
merge clause family, option precedence, and negative diagnostics. Isolated live tests cover preflight failures without
metadata changes, cosmetic expression equivalence, `name` and `off` modes, delete/update/merge effects, native CHECK
enforcement, and online/generated parity. `make build`, `make integration`, and `make build INTEGRATION=1` must pass
before the corresponding API rows are marked supported.

## Designed follow-up: explicit schema evolution

An explicit merge evolution must name the expected post-commit Structure schema and lower to the native merge
builder's `withSchemaEvolution()`. An explicit append evolution must be a separate typed append effect because native
`mergeSchema` is a DataFrame writer option. Both operations must validate an accepted pre-commit schema before the
first effect and the exact named post-commit schema after their commit. They must not enable session-wide auto-merge,
silently accept unknown columns, install constraints, or treat overwrite schema replacement as append evolution.
