from collections.abc import Callable

from ..authoring.Authoring import Equality, Row, Scan, ScanPlan, StateExpression, join, state, state_expression


def inner_join(relation: Row, *, on: Equality) -> Row:
    """Attach a finite keyed inner join to the active Iterable step."""
    return join("inner", relation, on)


def left_join(relation: Row, *, on: Equality) -> Row:
    """Attach a finite keyed left join to the active Iterable step."""
    return join("left", relation, on)


def scan(
    *,
    initial: tuple[int | float, ...],
    output: object,
    next: tuple[StateExpression, ...] | Callable[..., tuple[StateExpression, ...]],
) -> ScanPlan:
    """Capture a finite ordered state scan whose output is a schema instance."""
    if not initial:
        raise TypeError("Iterable scan requires at least one initial state value.")
    values = next(*(state[index] for index in range(len(initial)))) if callable(next) else next
    if not isinstance(values, tuple) or len(values) != len(initial):
        raise TypeError("Iterable scan next must return one state expression per initial state value.")
    return ScanPlan(output, Scan(initial, tuple(state_expression(value) for value in values)))
