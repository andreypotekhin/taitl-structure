from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from structure.plugin.api.v1.model.HookPlan import HookPlan
from structure.plugin.api.v1.model.StepInputPlan import StepInputPlan
from structure.plugin.api.v1.model.StepResultPlan import StepResultPlan
from structure.plugin.api.v1.model.TransformMemberOrigin import TransformMemberOrigin


@dataclass(frozen=True)
class StepPlan:
    """Core-owned step lifecycle facts plus an opaque plugin body."""

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
    plugin_body: object | None = None

    if TYPE_CHECKING:
        # Deprecated compile_transform(...) returns Core-private subclasses
        # carrying these payloads. They are intentionally absent from normal
        # plugin plans at runtime.
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
