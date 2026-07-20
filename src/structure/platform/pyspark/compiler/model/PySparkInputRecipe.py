from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema, StreamingMode
from structure.platform.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkInputRecipe:
    name: str
    schema: type[Schema]
    ordinal: int
    validation: PySparkValidationRecipe
    streaming: StreamingMode = StreamingMode.NO
    aliases: tuple[str, ...] = ()
