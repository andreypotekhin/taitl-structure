# Finite Iterable Starter Plugin

This example is a small, separately packaged Structure plugin that demonstrates the public Plugin API without
importing `structure.core`. It is a plugin-author starter, not a supported end-user target.

The package mirrors the bundled PySpark plugin's focused application layout on a deliberately smaller scale:

- `IterablePlugin.py` is the entry-point object and owns plugin identity.
- `api/` assembles one negotiated `PluginAPI` façade.
- `dsl/` owns the target-specific authoring plans.
- `compiler/` lowers a declared iterable operation into an opaque payload.
- `execution/` interprets that payload for finite mappings.
- `schema/`, `authoring/`, `capabilities/`, and `serialization/` supply the remaining v1 facets.

## Authoring a transform

An author imports target-neutral declarations from Structure and the operation vocabulary from this package:

    from structure import Schema, Transform, input, output, transform
    from structure_iterable import projection

    @transform(target="iterable")
    class ProjectOrders(Transform):
        operation = projection(fields={"order": "id"})

The compiler lowers the class-owned plan to an opaque payload. Declare an `input(...)` field when the plan consumes
rows, then provide its list, tuple, or one-shot generator as a named transform constructor argument. The iterable
executor materializes the finite input before evaluation so `collect()` is repeatable.

## Supported starter operations

- `projection(fields={output_name: source_name})`
- `inner_join(left=..., right=..., left_on=..., right_on=...)`
- `left_join(left=..., right=..., left_on=..., right_on=...)`
- `grouped(group_by=(...), aggregates={"total": {"sum": "amount"}, "count": {"count": None}})`
- `recurrence(initial=(...), output=..., next=(...))` for finite ordered state recurrences

The fixture intentionally has no generated-code, streaming, broad schema, or step-body DSL support. A production
plugin should replace its minimal schema and capability facets with target-specific implementations and publish its
own user documentation.

For example, a transform can express Fibonacci without baking it into the plugin:

    from structure import Transform, transform
    from structure_iterable import recurrence, state

    class SequenceRow(Schema):
        index: int

    class FibonacciRow(SequenceRow):
        fibonacci: int

    @transform(target="iterable")
    class Fibonacci(Transform):
        rows = input(SequenceRow)
        result = output(FibonacciRow)
        operation = recurrence(
            initial=(0, 1),
            output=state[0],
            next=lambda previous, current: (current, previous + current),
        )

With one declared input and output, `recurrence(...)` infers the input name and the one output field absent from the
input schema—`rows` and `fibonacci` above. Supply `input=` or `value=` only when that inference is ambiguous.
`next=` may be a tuple of `state[...]` expressions or a declaration-time lambda whose positional arguments are those
state values; it must return the next-state tuple.
It is intentionally an Iterable-only, finite ordered demonstration: its input `index` values must be exactly
`0, 1, 2, ...`; it is not Structure's future portable scan API.
