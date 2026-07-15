from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CachePlan:
    storage_level: tuple[bool, bool, bool, bool, int] | None = None
