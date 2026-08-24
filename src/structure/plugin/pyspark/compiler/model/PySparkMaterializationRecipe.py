from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PySparkPersistRecipe:
    storage_level: tuple[bool, bool, bool, bool, int] | None = None


@dataclass(frozen=True)
class PySparkUnpersistRecipe:
    blocking: bool = False


@dataclass(frozen=True)
class PySparkCheckpointRecipe:
    eager: bool = True
