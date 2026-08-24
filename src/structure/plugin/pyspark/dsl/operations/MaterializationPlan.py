from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PersistPlan:
    storage_level: tuple[bool, bool, bool, bool, int] | None = None


@dataclass(frozen=True)
class UnpersistPlan:
    blocking: bool = False


@dataclass(frozen=True)
class CheckpointPlan:
    eager: bool = True
