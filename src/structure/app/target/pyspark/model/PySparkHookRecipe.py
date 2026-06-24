from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.transforms.SchemaMode import SchemaMode


@dataclass(frozen=True)
class PySparkHookRecipe:
    name: str
    phase: str
    target: str
    pass_inputs: bool
    schema_mode: SchemaMode
    project_output: bool
    streaming_safe: bool
