# V7 Grouped Mode

## Purpose

This specification adds PySpark-named grouped `mode(...)` while making deterministic ties portable across Structure's
PySpark 3.5.x/4.0.x baseline.

## Public API

`mode(value, deterministic=False)` is an aggregate expression and must follow one `group_by(...)`, `rollup(...)`,
`cube(...)`, or `grouping_sets(...)` declaration in the step. It is used in a normal declared aggregate result:

    group_by(order.customer_id)
    return CustomerSummary(
        customer_id=order.customer_id,
        preferred_category=mode(order.category, deterministic=True),
    )

The candidate value must be an orderable scalar for `deterministic=True`; the default accepts all target-supported
mode value types. It returns the candidate type and is nullable when the group has no non-null candidate.

## Tie Semantics

`deterministic=False` is the PySpark-compatible default: tied most-frequent values may yield any tied candidate.
`deterministic=True` returns the lowest tied candidate using ascending target ordering. PySpark 4.0 lowers this directly
through `functions.mode(value, deterministic=True)`. PySpark 3.5 lowers a compiler-visible candidate-count and
tie-selection aggregate that produces the same result.

The first release is batch-only. Global mode, streaming mode, and mode inside a scalar lambda or window are rejected.

## Compiler Contract and Evidence

The aggregate plan records `mode` and its deterministic flag. The PySpark 3.5 compatibility lowering remains within the
same aggregate recipe and is visible in generated source, explain, and traceability; it does not use raw SQL, driver
collection, Python UDFs, or a hidden action. Tests cover grouped unique values, ties, nulls, non-orderable deterministic
input, placement errors, target rendering, online/generated parity, and live 3.5/4.0 equivalence.
