"""Runtime scalar variables supplied to each transform invocation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from types import UnionType
from typing import TYPE_CHECKING, Union, get_args, get_origin

if TYPE_CHECKING:
    from structure.core.dsl.model.transforms.Transform import Transform


class _Unset:
    def __repr__(self) -> str:
        return "UNSET"


UNSET = _Unset()
_SCALAR_TYPES = (bool, int, float, str, bytes, Decimal, date, datetime)


@dataclass(frozen=True)
class VariableReference:
    """A stable typed reference captured in a compiled expression."""

    name: str
    python_type: type
    nullable: bool = False
    precision: int | None = None
    scale: int | None = None


@dataclass(frozen=True)
class VariableDeclaration:
    """A typed runtime scalar bound to a Transform invocation."""

    python_type: type
    default: object = UNSET
    nullable: bool = False
    precision: int | None = None
    scale: int | None = None
    name: str = ""

    @property
    def required(self) -> bool:
        return self.default is UNSET

    def __set_name__(self, owner: type[Transform], name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: Transform | None, owner: type[Transform] | None = None) -> object:
        if instance is None:
            return self
        from structure.plugin.api.v1.model import current_symbolic_context

        context = current_symbolic_context()
        if context is not None:
            factory = getattr(context, "variable_reference", None)
            if factory is None:
                raise TypeError(f"The active compiler does not support runtime variable {self.name!r}")
            reference = instance._structure_bound_variables.get(self.name)
            if isinstance(reference, VariableReference):
                return factory(reference)
            return factory(
                VariableReference(
                    self.name,
                    self.python_type,
                    nullable=self.nullable,
                    precision=self.precision,
                    scale=self.scale,
                )
            )
        value = instance._structure_bound_variables.get(self.name, self.default)
        if value is UNSET:
            raise TypeError(f"Runtime variable {self.name!r} is not bound on {type(instance).__name__}")
        return value

    def __set__(self, instance: Transform, value: object) -> None:
        raise AttributeError(f"Runtime variable {self.name!r} is immutable; bind a new transform invocation")

    def validate(self, value: object) -> object:
        if value is None:
            if self.nullable:
                return value
            raise TypeError(f"Runtime variable {self.name!r} does not allow None")
        if type(value) is not self.python_type:
            raise TypeError(
                f"Runtime variable {self.name!r} requires {self.python_type.__name__}, got {type(value).__name__}"
            )
        if self.python_type is Decimal:
            assert isinstance(value, Decimal)
            if not value.is_finite():
                raise TypeError(f"Runtime variable {self.name!r} requires a finite Decimal")
            digits = len(value.as_tuple().digits)
            exponent = value.as_tuple().exponent
            assert isinstance(exponent, int)
            scale = max(-exponent, 0)
            precision = max(digits, scale)
            if self.precision is None or self.scale is None or precision > self.precision or scale > self.scale:
                raise TypeError(
                    f"Runtime variable {self.name!r} Decimal does not fit decimal({self.precision},{self.scale})"
                )
        return value


def variable(
    python_type: object,
    *,
    default: object = UNSET,
    precision: int | None = None,
    scale: int | None = None,
) -> VariableDeclaration:
    """Declare a runtime scalar variable without specializing compiled code."""
    nullable = False
    origin = get_origin(python_type)
    if origin in {UnionType, Union}:
        choices = get_args(python_type)
        nullable = type(None) in choices
        scalar_choices = tuple(choice for choice in choices if choice is not type(None))
        if len(scalar_choices) != 1:
            raise TypeError("variable(...) accepts one scalar type, optionally combined with None")
        python_type = scalar_choices[0]
    if python_type not in _SCALAR_TYPES:
        raise TypeError("variable(...) requires bool, int, float, str, bytes, Decimal, date, or datetime")
    if python_type is Decimal:
        if not isinstance(precision, int) or isinstance(precision, bool) or not 1 <= precision <= 38:
            raise TypeError("Decimal variables require precision between 1 and 38")
        if not isinstance(scale, int) or isinstance(scale, bool) or not 0 <= scale <= precision:
            raise TypeError("Decimal variables require scale between 0 and precision")
    elif precision is not None or scale is not None:
        raise TypeError("precision and scale are supported only for Decimal variables")
    declaration = VariableDeclaration(
        python_type=python_type,
        default=default,
        nullable=nullable,
        precision=precision,
        scale=scale,
    )
    if default is not UNSET:
        declaration.validate(default)
    return declaration
