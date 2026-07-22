from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkInputRecipe:
    name: str
    schema: type[Schema]
    ordinal: int
    validation: PySparkValidationRecipe
    streaming: bool = False
    aliases: tuple[str, ...] = ()
