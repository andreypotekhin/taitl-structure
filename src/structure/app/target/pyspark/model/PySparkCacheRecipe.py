from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PySparkCacheRecipe:
    storage_level: tuple[bool, bool, bool, bool, int] | None = None
