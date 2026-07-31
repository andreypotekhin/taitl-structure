"""Output declarations for Structure transform classes."""

from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.aliases import alias as declaration_alias
from structure.core.dsl.model.transforms.InOutBinding import bind_inout


@dataclass(frozen=True)
class OutputBindings:
    """A named source mapping for output declarations on a transform graph.

    ``outputs = output(name=stage.output)`` keeps output schemas declared next
    to the transform contract while collecting graph source assignments in one
    constructor-style block.
    """

    bindings: tuple[tuple[str, object], ...]


@dataclass(frozen=True)
class OutputDeclaration:
    """A named relation produced by a transform.

    ``output(Schema)`` declares a public result of a transform.  Outputs can be
    selected in ``@step(output=...)`` bindings, renamed on invocation, or mapped
    from composed stage outputs.
    """

    schema: type[Schema]
    name: str = ""
    aliases: tuple[str, ...] = ()
    source: object | None = None

    def __set_name__(self, owner: type, name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: object | None, owner: type | None = None):
        return self

    def __or__(self, outputs: object):
        return bind_inout(self, outputs)

    def __ror__(self, inputs: object):
        return bind_inout(inputs, self)

    def alias(self, *names: str) -> OutputDeclaration:
        """Add alternate names accepted by transform composition and invocation."""
        return declaration_alias(self, names)
