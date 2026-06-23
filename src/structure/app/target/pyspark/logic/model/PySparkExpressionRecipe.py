from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from structure.app.dsl.logic.model.types.StructureType import StructureType


@dataclass(frozen=True)
class PySparkExpressionRecipe:
    kind: str
    type: StructureType | None
    nullable: bool
    data: Mapping[str, object]
    args: tuple["PySparkExpressionRecipe", ...] = ()
