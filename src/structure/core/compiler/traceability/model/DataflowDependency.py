from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class DataflowDependency:
    target: str
    sources: tuple[str, ...]
    operation: str
    step: str | None
    detail: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "detail": dict(sorted(self.detail.items())),
            "operation": self.operation,
            "sources": list(self.sources),
            "step": self.step,
            "target": self.target,
        }
