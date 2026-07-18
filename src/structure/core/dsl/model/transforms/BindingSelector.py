from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.InOutBinding import bind_inout
from structure.core.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.core.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.core.dsl.model.transforms.OutputDeclaration import OutputDeclaration

BindingRole = Literal["input", "lane", "output"]
SelectedDeclaration = InputDeclaration | LaneDeclaration | OutputDeclaration


@dataclass(frozen=True)
class BindingSelector:
    role: BindingRole
    declaration: SelectedDeclaration

    @property
    def name(self) -> str:
        return self.declaration.name

    @property
    def schema(self) -> type[Schema]:
        return self.declaration.schema

    def __or__(self, outputs: object):
        return bind_inout(self, outputs)

    def __ror__(self, inputs: object):
        return bind_inout(inputs, self)
