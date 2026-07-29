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
