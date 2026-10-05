from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StateBudgetPlan:
    """A transform memory declaration or the limits for one stateful operation."""

    max_rows: int | None = None
    max_state_bytes: int | None = None
    memory_source: str | None = None
    fallback_mb: int | None = None

    @property
    def is_memory_policy(self) -> bool:
        return self.memory_source is not None
