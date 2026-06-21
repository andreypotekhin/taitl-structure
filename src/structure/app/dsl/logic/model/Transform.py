from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
from types import TracebackType
from typing import Callable

from structure.app.dsl.logic.model.expressions import Expression, literal
from structure.app.dsl.logic.model.Schema import Structure

_current: ContextVar["CompileContext | None"] = ContextVar("structure_compile_context", default=None)


@dataclass(frozen=True)
class InputDeclaration:
    schema: type[Structure]
    name: str = ""

    def __set_name__(self, owner: type[Transform], name: str) -> None:
        object.__setattr__(self, "name", name)


class CompileContext:

    def __init__(self, *, step: str) -> None:
        self.step = step
        self.filters: list[Expression] = []
        self._token: Token[CompileContext | None] | None = None

    def __enter__(self) -> "CompileContext":
        self._token = _current.set(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._token is not None:
            _current.reset(self._token)


class ExprFunction:

    def __init__(self, function: Callable) -> None:
        self.function = function
        self.__name__ = function.__name__

    def __call__(self, *args, **kwargs):
        return literal(self.function(*args, **kwargs))

    def __get__(self, instance: object, owner: type | None = None):
        return self.__call__


class Transform:

    _structure_inputs: dict[str, InputDeclaration] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        inputs: dict[str, InputDeclaration] = {}
        for base in cls.__bases__:
            inputs.update(getattr(base, "_structure_inputs", {}))

        for value in cls.__dict__.values():
            if isinstance(value, InputDeclaration):
                inputs[value.name] = value

        cls._structure_inputs = inputs

    def __init__(self, **inputs: object) -> None:
        unknown = set(inputs) - set(self._structure_inputs)
        if unknown:
            allowed = ", ".join(self._structure_inputs)
            raise TypeError(
                f"{type(self).__name__} got unknown input(s): {', '.join(sorted(unknown))}. Allowed: {allowed}"
            )
        self._structure_bound_inputs = dict(inputs)
        self.schemas = None

    def run(self, session):
        return session.run(self)


def input(schema: type[Structure]) -> InputDeclaration:
    if not isinstance(schema, type) or not issubclass(schema, Structure):
        raise TypeError("input(...) requires a Structure schema class")
    return InputDeclaration(schema=schema)


def transform(target=None, **kwargs):
    allowed = {"validate_intermediate", "streaming_compatible"}
    unknown = set(kwargs) - allowed
    if unknown:
        raise TypeError(f"@transform got unknown option(s): {', '.join(sorted(unknown))}")

    def decorate(cls):
        if not issubclass(cls, Transform):
            raise TypeError("@transform classes must inherit from Transform")
        cls._structure_transform = True
        cls._structure_transform_options = dict(kwargs)
        return cls

    if target is None:
        return decorate
    if kwargs:
        return decorate(target)
    return decorate(target)


def expr_fn(function: Callable) -> ExprFunction:
    return ExprFunction(function)


def where(predicate: object) -> None:
    context = _current.get()
    if context is None:
        raise RuntimeError("where(...) can only be used inside a compiled Structure subtransform")
    context.filters.append(literal(predicate))
