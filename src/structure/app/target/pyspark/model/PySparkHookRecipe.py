from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.transforms.SchemaMode import SchemaMode


@dataclass(frozen=True)
class PySparkHookRecipe:
    name: str
    phase: str
    target: str
    lanes: tuple[str, ...]
    outputs: tuple[str, ...]
    pass_inputs: bool
    schema_mode: SchemaMode
    project_output: bool
    streaming_safe: bool
    target_backend: tuple[str, ...] = ("pyspark",)
    target_defaulted: bool = True

    @property
    def lane(self) -> str:
        return self.lanes[0]
