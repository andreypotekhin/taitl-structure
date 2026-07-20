from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from structure.platform.api.v1.model.HookPlan import HookPlan


@dataclass(frozen=True)
class StepResultPlan:
    """A Core-resolved step result and hook placement."""

    schema: Any
    lane: str
    frame: str
    ordinal: int
    after_hooks: tuple[HookPlan, ...] = ()

    if TYPE_CHECKING:
        # See StepPlan's compatibility note.
        @property
        def projection(self) -> tuple[Any, ...]: ...

        @property
        def aggregate(self) -> Any: ...
