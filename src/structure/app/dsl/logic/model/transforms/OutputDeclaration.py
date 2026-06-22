from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from structure.app.dsl.logic.model.schemas.Structure import Structure

if TYPE_CHECKING:
    from structure.app.dsl.logic.model.transforms.Transform import Transform


@dataclass(frozen=True)
class OutputDeclaration:
    schema: type[Structure]
    name: str = ""

    def __set_name__(self, owner: type[Transform], name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: Transform | None, owner: type[Transform] | None = None):
        return self
