from __future__ import annotations

from dataclasses import dataclass

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.StreamingMode import StreamingMode
from structure.platform.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkInputRecipe:
    name: str
    schema: type[Schema]
    ordinal: int
    validation: PySparkValidationRecipe
    streaming: StreamingMode = StreamingMode.NO
    aliases: tuple[str, ...] = ()
