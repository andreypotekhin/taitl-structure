from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from structure.plugin.pyspark.dsl.types import StructureType


@dataclass(frozen=True)
class PySparkExpressionRecipe:
    kind: str
    type: StructureType | None
    nullable: bool
    data: Mapping[str, object]
    args: tuple["PySparkExpressionRecipe", ...] = ()
