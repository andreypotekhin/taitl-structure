from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ArtifactCacheReport:
    entries: int
    hits: int
    misses: int
    loaded: int
