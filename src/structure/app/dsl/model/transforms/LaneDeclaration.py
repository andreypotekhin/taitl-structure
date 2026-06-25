from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.InOutBinding import bind_inout


@dataclass(frozen=True)
class LaneDeclaration:
    schema: type[Structure]
    name: str = ""

    def __set_name__(self, owner: type, name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: object | None, owner: type | None = None):
        return self

    def __or__(self, outputs: object):
        return bind_inout(self, outputs)

    def __ror__(self, inputs: object):
        return bind_inout(inputs, self)
