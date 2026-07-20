from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from structure.core.dsl.model.expr.InputScope import InputScope
from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.aliases import alias as declaration_alias
from structure.core.dsl.model.transforms.InOutBinding import bind_inout
from structure.core.dsl.model.transforms.StreamingMode import StreamingMode

if TYPE_CHECKING:
    from structure.core.dsl.model.transforms.Transform import Transform


@dataclass(frozen=True)
class InputDeclaration:
    schema: type[Schema]
    name: str = ""
    streaming: StreamingMode = StreamingMode.NO
    aliases: tuple[str, ...] = ()

    def __set_name__(self, owner: type[Transform], name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: Transform | None, owner: type[Transform] | None = None):
        if instance is None:
            return self
        from structure.core.compiler.symbolic_execution.api import SymbolicExecution

        context = SymbolicExecution().current()()
        scope = InputScope(name=self.name, schema=self.schema)
        if context is None:
            return scope
        return context.register_relation_scope(self.name, scope)

    def __or__(self, outputs: object):
        return bind_inout(self, outputs)

    def __ror__(self, inputs: object):
        return bind_inout(inputs, self)

    def alias(self, *names: str) -> InputDeclaration:
        return declaration_alias(self, names)
