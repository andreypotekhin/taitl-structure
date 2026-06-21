from __future__ import annotations

from dataclasses import dataclass

from structure.app.backend.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe
from structure.app.dsl.logic.model.schemas.Structure import Structure


@dataclass(frozen=True)
class PySparkInputRecipe:
    name: str
    schema: type[Structure]
    ordinal: int
    validation: PySparkValidationRecipe
