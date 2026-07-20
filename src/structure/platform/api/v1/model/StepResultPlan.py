from dataclasses import dataclass
from typing import Any

from structure.platform.api.v1.model.HookPlan import HookPlan


@dataclass(frozen=True)
class StepResultPlan:
    """A Core-resolved step result with plugin-owned projection details."""

    schema: Any
    lane: str
    frame: str
    projection: tuple[Any, ...]
    ordinal: int
    aggregate: Any = None
    after_hooks: tuple[HookPlan, ...] = ()
