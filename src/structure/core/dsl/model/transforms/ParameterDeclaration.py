"""Scalar transform configuration declarations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from structure.core.dsl.model.transforms.Transform import Transform


@dataclass(frozen=True)
class ParameterDeclaration:
    """One scalar transform parameter with a default value."""

    default: object
    name: str = ""

    def __set_name__(self, owner: type[Transform], name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: Transform | None, owner: type[Transform] | None = None) -> object:
        if instance is None:
            return self
        return instance._structure_bound_parameters.get(self.name, self.default)

    def __invert__(self) -> NegatedParameter:
        """Defer Boolean negation until a composed invocation is compiled."""
        if not isinstance(self.default, bool):
            raise TypeError("Only Boolean transform parameters support ~parameter")
        return NegatedParameter(self)

    def __bool__(self) -> bool:
        raise TypeError("A parameter declaration is not a value; use ~parameter for deferred Boolean negation")


@dataclass(frozen=True)
class NegatedParameter:
    parameter: ParameterDeclaration

    def __invert__(self) -> ParameterDeclaration:
        return self.parameter

    def __bool__(self) -> bool:
        raise TypeError("A parameter expression is not a value; it is resolved when the transform is compiled")
