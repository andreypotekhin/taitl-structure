from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.model.transforms.SchemaMode import SchemaMode


@dataclass(frozen=True)
class HookPlan:
    name: str
    phase: str
    target: str
    lane: InputDeclaration | OutputDeclaration
    pass_inputs: bool = False
    schema_mode: SchemaMode = SchemaMode.STRICT
    project_output: bool = False
    streaming_safe: bool = False
