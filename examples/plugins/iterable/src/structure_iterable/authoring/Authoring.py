from __future__ import annotations

from collections.abc import Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any, cast

from structure.plugin.api.v1 import AuthoringAPI as AuthoringAPIV1
from structure.plugin.api.v1 import StepAuthoringCapture, StepAuthoringRequest, StepAuthoringResult


@dataclass(frozen=True)
class Field:
    row: str
    name: str
    nullable: bool = False

    def __eq__(self, other: object) -> "Equality":  # type: ignore[override]
        if not isinstance(other, Field):
            raise TypeError("Iterable join keys must compare two schema fields.")
        return Equality(self, other)


@dataclass(frozen=True)
class Equality:
    left: Field
    right: Field


@dataclass
class Row:
    lane: str
    schema: object
    nullable: bool = False

    def __getattr__(self, name: str) -> Field:
        fields = getattr(self.schema, "_structure_fields", {})
        if name not in fields:
            raise AttributeError(name)
        definition = fields[name]
        return Field(self.lane, name, self.nullable or definition.nullable)


@dataclass(frozen=True)
class Join:
    kind: str
    relation: str
    left: Field
    right: Field


@dataclass(frozen=True)
class Projection:
    schema: object
    values: dict[str, object]


@dataclass(frozen=True)
class IterableStepBody:
    joins: tuple[Join, ...]
    projections: tuple[Projection, ...]
    scan: "Scan | None" = None


class StateExpression:
    def __add__(self, other: object) -> "BinaryStateExpression":
        return BinaryStateExpression("add", self, state_expression(other))


@dataclass(frozen=True)
class StateValue(StateExpression):
    ordinal: int


@dataclass(frozen=True)
class LiteralStateExpression(StateExpression):
    value: int | float


@dataclass(frozen=True)
class BinaryStateExpression(StateExpression):
    operation: str
    left: StateExpression
    right: StateExpression


class State:
    def __getitem__(self, ordinal: int) -> StateValue:
        if not isinstance(ordinal, int) or ordinal < 0:
            raise TypeError("Iterable state indexes must be non-negative integers.")
        return StateValue(ordinal)


state = State()


@dataclass(frozen=True)
class Scan:
    initial: tuple[int | float, ...]
    next: tuple[StateExpression, ...]


@dataclass(frozen=True)
class ScanPlan:
    output: object
    scan: Scan


_session: ContextVar["IterableStepSession | None"] = ContextVar("iterable_step_session", default=None)


class Authoring(AuthoringAPIV1):
    """Supplies symbolic schema rows and captures one declarative Iterable step."""

    def open_step(self, request: StepAuthoringRequest) -> "IterableStepSession":
        return IterableStepSession(request)

    def result_arguments(self, results: tuple[StepAuthoringResult, ...]) -> tuple[object, ...]:
        return tuple(Row(result.lane, result.schema) for result in results)

    def rewrite_body(self, body: object, *, frames: Mapping[str, str]) -> object:
        return body


class IterableStepSession:
    def __init__(self, request: StepAuthoringRequest) -> None:
        self.request = request
        self._arguments = tuple(Row(item.lane, item.schema) for item in request.inputs)
        self.joins: list[Join] = []
        self._token: Token[IterableStepSession | None] | None = None

    def __enter__(self) -> "IterableStepSession":
        self._token = _session.set(self)
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self._token is not None:
            _session.reset(self._token)

    def arguments(self) -> tuple[object, ...]:
        return self._arguments

    def validate(self) -> tuple[object, ...]:
        return ()

    def capture(self, value: object) -> StepAuthoringCapture:
        if isinstance(value, ScanPlan):
            return StepAuthoringCapture(IterableStepBody((), (self._projection(value.output, self.request.results[0]),), value.scan))
        values = value if isinstance(value, tuple) else (value,)
        if len(values) != len(self.request.results):
            raise TypeError(
                f"Iterable step {self.request.name!r} returned {len(values)} result(s); "
                f"declare and return {len(self.request.results)} result(s)."
            )
        projections = tuple(self._projection(item, result) for item, result in zip(values, self.request.results, strict=True))
        return StepAuthoringCapture(IterableStepBody(tuple(self.joins), projections))

    def _projection(self, value: object, result: StepAuthoringResult) -> Projection:
        schema = cast(type, result.schema)
        if not isinstance(value, schema):
            expected = schema.__name__
            raise TypeError(f"Iterable step {self.request.name!r} must return {expected}(...) for {result.lane!r}.")
        values = cast(Any, value)._structure_values
        allowed = (Field, StateExpression, str, int, float, bool, type(None))
        if not all(isinstance(item, allowed) for item in values.values()):
            raise TypeError("Iterable schema values must be row fields or scalar literals.")
        for name, item in values.items():
            field = cast(Any, schema)._structure_fields[name]
            if not field.nullable and (item is None or isinstance(item, Field) and item.nullable):
                raise TypeError(
                    f"Iterable output {schema.__name__}.{name} is non-nullable but its assigned value may be null. "
                    "Declare field(nullable=True) or use a non-null source."
                )
        return Projection(result.schema, dict(values))


def join(kind: str, relation: Row, condition: Equality) -> Row:
    session = _session.get()
    if session is None:
        raise RuntimeError("Iterable joins can only be used inside a compiled @step method.")
    if relation.lane not in {item.lane for item in session.request.inputs[1:]}:
        raise TypeError("Iterable joins may only join a secondary step input.")
    if condition.left.row == relation.lane:
        left, right = condition.right, condition.left
    elif condition.right.row == relation.lane:
        left, right = condition.left, condition.right
    else:
        raise TypeError("Iterable join condition must compare the joined relation to another step row.")
    session.joins.append(Join(kind, relation.lane, left, right))
    if kind == "left":
        relation.nullable = True
    return relation

def state_expression(value: object) -> StateExpression:
    if isinstance(value, StateExpression):
        return value
    if isinstance(value, int | float):
        return LiteralStateExpression(value)
    raise TypeError("Iterable scan state may use state values and numeric literals only.")
