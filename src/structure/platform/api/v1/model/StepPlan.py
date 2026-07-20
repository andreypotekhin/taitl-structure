from dataclasses import dataclass
from typing import Any

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
    filters: tuple[Any, ...]
    projection: tuple[Any, ...]
    ordinal: int
    aggregate: Any = None
    joins: tuple[Any, ...] = ()
    operations: tuple[Any, ...] = ()
    before_hooks: tuple[HookPlan, ...] = ()
    after_hooks: tuple[HookPlan, ...] = ()
    inputs: tuple[StepInputPlan, ...] = ()
    results: tuple[StepResultPlan, ...] = ()
    options: dict[str, object] | None = None
    origin: TransformMemberOrigin | None = None
    platform_body: object | None = None
