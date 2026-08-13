"""Input declarations for Structure transform classes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.aliases import alias as declaration_alias
from structure.core.dsl.model.transforms.InOutBinding import bind_inout

if TYPE_CHECKING:
    from structure.core.dsl.model.transforms.Transform import Transform


@dataclass(frozen=True)
class InputDeclaration:
    """A named external relation consumed by a transform.

    Users create input declarations with ``input(Schema)`` on a ``Transform``
    class.  During symbolic step authoring, reading the descriptor returns the
    active plugin's row scope for the declared schema.
    """

    schema: type[Schema]
    name: str = ""
    streaming: bool = False
    optional: bool = False
    aliases: tuple[str, ...] = ()
    streaming_declared: bool = False

    def __set_name__(self, owner: type[Transform], name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: Transform | None, owner: type[Transform] | None = None):
        if instance is None:
            return self
        from structure.core.compiler.symbolic_execution.api import SymbolicExecution

        context = SymbolicExecution().current()()
        if context is None:
            raise RuntimeError(f"{self.name} can only be read while a plugin is authoring a Structure step")
        scope = getattr(context, "input_scope", None)
        if not callable(scope):
            raise TypeError("The selected plugin does not provide relation scopes")
        return context.register_relation_scope(self.name, scope(name=self.name, schema=self.schema))

    def __or__(self, outputs: object):
        return bind_inout(self, outputs)

    def __ror__(self, inputs: object):
        return bind_inout(inputs, self)

    def alias(self, *names: str) -> InputDeclaration:
        """Add alternate invocation names for this input declaration."""
        return declaration_alias(self, names)
