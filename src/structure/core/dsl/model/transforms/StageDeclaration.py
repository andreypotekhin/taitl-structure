"""Stage declarations for class-based transform composition."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from structure.core.dsl.model.schemas.Schema import Schema

if TYPE_CHECKING:
    from structure.core.dsl.model.transforms.Transform import Transform


@dataclass(frozen=True)
class StageOutputReference:
    """A reference to one output of a composed stage."""

    stage: "StageDeclaration"
    name: str
    schema: type[Schema]


@dataclass(frozen=True)
class StageDeclaration:
    """A named transform invocation embedded in another transform.

    Users create stage declarations with ``stage(OtherTransform(...))`` or by
    assigning a transform invocation directly in a transform class, then map
    outputs with ``output(..., stage_name.output_name)``.
    """

    invocation: Transform
    name: str = ""

    def __set_name__(self, owner: type, name: str) -> None:
        object.__setattr__(self, "name", name)

    def __get__(self, instance: object | None, owner: type | None = None):
        return self

    def __getattr__(self, name: str) -> StageOutputReference:
        """Return a typed reference to a declared output on the staged transform."""
        return _output_reference(self, name)


def _output_reference(stage: StageDeclaration, name: str) -> StageOutputReference:
    """Build a typed reference to one output of a stage."""
    outputs = getattr(type(stage.invocation), "_structure_outputs", {})
    output = outputs.get(name)
    if output is None:
        transform = type(stage.invocation).__name__
        allowed = ", ".join(outputs) or "none"
        raise AttributeError(f"{transform} has no output {name!r}. Available outputs: {allowed}")
    return StageOutputReference(stage=stage, name=name, schema=output.schema)
