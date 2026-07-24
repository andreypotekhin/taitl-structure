from collections.abc import Callable, Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class IterablePlan:
    """A vendor-owned declarative transform operation."""

    payload: Mapping[str, object]


def projection(*, fields: Mapping[str, str], input: str = "input") -> IterablePlan:
    return IterablePlan({"operation": "project", "input": input, "fields": dict(fields)})


def inner_join(*, left: str, right: str, left_on: str, right_on: str) -> IterablePlan:
    return _join("inner_join", left, right, left_on, right_on)


def left_join(*, left: str, right: str, left_on: str, right_on: str) -> IterablePlan:
    return _join("left_join", left, right, left_on, right_on)


def grouped(*, group_by: tuple[str, ...], aggregates: Mapping[str, Mapping[str, object]], input: str = "input") -> IterablePlan:
    return IterablePlan({"operation": "aggregate", "input": input, "group_by": group_by, "aggregates": dict(aggregates)})


class StateExpression:
    """An arithmetic expression over one recurrence state vector."""

    def plan(self) -> Mapping[str, object]:
        raise NotImplementedError

    def __add__(self, other: object) -> "BinaryExpression":
        return BinaryExpression("add", self, _expression(other))

    def __sub__(self, other: object) -> "BinaryExpression":
        return BinaryExpression("subtract", self, _expression(other))

    def __mul__(self, other: object) -> "BinaryExpression":
        return BinaryExpression("multiply", self, _expression(other))

    def __truediv__(self, other: object) -> "BinaryExpression":
        return BinaryExpression("divide", self, _expression(other))


@dataclass(frozen=True)
class StateValue(StateExpression):
    index: int

    def plan(self) -> Mapping[str, object]:
        return {"state": self.index}


@dataclass(frozen=True)
class LiteralExpression(StateExpression):
    value: int | float

    def plan(self) -> Mapping[str, object]:
        return {"literal": self.value}


@dataclass(frozen=True)
class BinaryExpression(StateExpression):
    operation: str
    left: StateExpression
    right: StateExpression

    def plan(self) -> Mapping[str, object]:
        return {self.operation: [self.left.plan(), self.right.plan()]}


class State:
    """Addresses a value in a recurrence's prior state."""

    def __getitem__(self, index: int) -> StateValue:
        if not isinstance(index, int) or index < 0:
            raise ValueError("Iterable recurrence state indexes must be non-negative integers.")
        return StateValue(index)


state = State()


def recurrence(
    *,
    initial: tuple[int | float, ...],
    output: StateExpression,
    next: tuple[StateExpression, ...] | Callable[..., tuple[StateExpression, ...]],
    index: str = "index",
    value: str | None = None,
    input: str | None = None,
) -> IterablePlan:
    """Emits a finite ordered recurrence from caller-provided index rows."""
    if not initial:
        raise ValueError("Iterable recurrences require at least one initial state value.")
    transitions = _transitions(next, len(initial))
    if len(transitions) != len(initial):
        raise ValueError("Iterable recurrences require one next-state expression per initial value.")
    if not isinstance(index, str) or (input is not None and not isinstance(input, str)) or (
        value is not None and not isinstance(value, str)
    ):
        raise TypeError("Iterable recurrence index, input, and value names must be strings.")
    payload: dict[str, object] = {
        "operation": "recurrence",
        "index": index,
        "initial": list(initial),
        "output": output.plan(),
        "next": [expression.plan() for expression in transitions],
    }
    if input is not None:
        payload["input"] = input
    if value is not None:
        payload["value"] = value
    return IterablePlan(payload)


def _join(operation: str, left: str, right: str, left_on: str, right_on: str) -> IterablePlan:
    return IterablePlan({"operation": operation, "left": left, "right": right, "left_on": left_on, "right_on": right_on})


def _expression(value: object) -> StateExpression:
    if isinstance(value, StateExpression):
        return value
    if isinstance(value, int | float):
        return LiteralExpression(value)
    raise TypeError("Iterable recurrence expressions may use state values and numeric literals only.")


def _transitions(
    next: tuple[StateExpression, ...] | Callable[..., tuple[StateExpression, ...]], state_size: int
) -> tuple[StateExpression, ...]:
    transitions = next(*(state[index] for index in range(state_size))) if callable(next) else next
    if not isinstance(transitions, tuple) or not all(isinstance(expression, StateExpression) for expression in transitions):
        raise TypeError("Iterable recurrence next must be state expressions or a lambda returning their tuple.")
    return transitions
