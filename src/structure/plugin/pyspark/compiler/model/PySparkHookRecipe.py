from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import SchemaMode
from structure.plugin.api.v1 import TransformMemberOrigin


@dataclass(frozen=True)
class PySparkHookRecipe:
    name: str
    phase: str
    target: str
    lanes: tuple[str, ...]
    outputs: tuple[str, ...]
    sources: tuple[str, ...]
    schema_mode: SchemaMode
    project_output: bool
    streaming_safe: bool
    target_backend: tuple[str, ...] = ("pyspark",)
    target_defaulted: bool = True
    target_platform: str | None = None
    origin: TransformMemberOrigin | None = None

    @property
    def lane(self) -> str:
        return self.lanes[0]
