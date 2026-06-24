from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Structure import Structure


@dataclass(frozen=True)
class OutputDeclaration:
    schema: type[Structure]
    name: str = ""

    def __set_name__(self, owner: type, name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: object | None, owner: type | None = None):
        return self
