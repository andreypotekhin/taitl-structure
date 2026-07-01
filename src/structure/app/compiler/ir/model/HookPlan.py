from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.model.transforms.SchemaMode import SchemaMode

HookDeclaration = InputDeclaration | LaneDeclaration | OutputDeclaration


@dataclass(frozen=True)
class HookPlan:
    name: str
    phase: str
    target: str
    lanes: tuple[HookDeclaration, ...]
    outputs: tuple[HookDeclaration, ...]
    pass_inputs: bool = False
    schema_mode: SchemaMode = SchemaMode.STRICT
    project_output: bool = False
    streaming_safe: bool = False
    target_backend: tuple[str, ...] = ("pyspark",)
    target_defaulted: bool = True

    @property
    def lane(self) -> HookDeclaration:
        return self.lanes[0]
