from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PySparkOptimizationTrace:
    kind: str
    eliminated_steps: tuple[str, ...]
    detail: str
