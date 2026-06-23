from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.target.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkInputRecipe:
    name: str
    schema: type[Structure]
    ordinal: int
    validation: PySparkValidationRecipe
