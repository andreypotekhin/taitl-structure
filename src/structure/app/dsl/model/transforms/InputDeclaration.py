from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from structure.app.dsl.model.expr.InputScope import InputScope
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.InOutBinding import bind_inout

if TYPE_CHECKING:
    from structure.app.dsl.model.transforms.Transform import Transform


@dataclass(frozen=True)
class InputDeclaration:
    schema: type[Structure]
    name: str = ""

    def __set_name__(self, owner: type[Transform], name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: Transform | None, owner: type[Transform] | None = None):
        if instance is None:
            return self
        from structure.app.compiler.symbolic_execution.model.CompileContext import current_context

        context = current_context()
        scope = InputScope(name=self.name, schema=self.schema)
        if context is None:
            return scope
        return context.register_relation_scope(self.name, scope)

    def __or__(self, outputs: object):
        return bind_inout(self, outputs)

    def __ror__(self, inputs: object):
        return bind_inout(inputs, self)
