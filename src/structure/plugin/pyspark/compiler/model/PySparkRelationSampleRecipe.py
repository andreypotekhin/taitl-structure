from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PySparkRelationSampleRecipe:
    fraction: float
    with_replacement: bool
    seed: int | None
    reproducible: bool
