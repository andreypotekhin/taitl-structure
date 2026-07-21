from dataclasses import dataclass
from typing import Any

from structure.plugin.api.v1.model.TransformMemberOrigin import TransformMemberOrigin


@dataclass(frozen=True)
class HookPlan:
    """A Core-resolved structural hook placement with plugin-opaque declarations."""

    name: str
    phase: str
    target: str
    lanes: tuple[Any, ...]
    outputs: tuple[Any, ...]
    sources: tuple[str, ...] = ()
    schema_mode: Any = "strict"
    project_output: bool = False
    streaming_safe: bool = False
    target_backend: tuple[str, ...] = ("pyspark",)
    target_defaulted: bool = True
    target_platform: str | None = None
    origin: TransformMemberOrigin | None = None

    @property
    def lane(self) -> Any:
        return self.lanes[0]
