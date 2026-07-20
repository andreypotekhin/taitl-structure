from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from structure.platform.api.v1.model.HookPlan import HookPlan
from structure.platform.api.v1.model.StepInputPlan import StepInputPlan
from structure.platform.api.v1.model.StepResultPlan import StepResultPlan
from structure.platform.api.v1.model.TransformMemberOrigin import TransformMemberOrigin


@dataclass(frozen=True)
class StepPlan:
    """Core-owned step lifecycle facts plus an opaque platform body."""

    name: str
    input_schema: Any
    output_schema: Any
    source: str
    source_scope: str
    input_lane: str
    output_lane: str
    ordinal: int
    before_hooks: tuple[HookPlan, ...] = ()
    after_hooks: tuple[HookPlan, ...] = ()
    inputs: tuple[StepInputPlan, ...] = ()
    results: tuple[StepResultPlan, ...] = ()
    options: dict[str, object] | None = None
    origin: TransformMemberOrigin | None = None
    platform_body: object | None = None

    if TYPE_CHECKING:
        # Deprecated compile_transform(...) returns Core-private subclasses
        # carrying these payloads. They are intentionally absent from normal
        # platform plans at runtime.
        @property
        def filters(self) -> tuple[Any, ...]: ...

        @property
        def projection(self) -> tuple[Any, ...]: ...

        @property
        def aggregate(self) -> Any: ...

        @property
        def joins(self) -> tuple[Any, ...]: ...

        @property
        def operations(self) -> tuple[Any, ...]: ...
