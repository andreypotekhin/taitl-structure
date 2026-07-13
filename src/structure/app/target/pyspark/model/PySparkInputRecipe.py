from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Schema import Schema
from structure.app.dsl.model.transforms.StreamingMode import StreamingMode
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkInputRecipe:
    name: str
    schema: type[Schema]
    ordinal: int
    validation: PySparkValidationRecipe
    streaming: StreamingMode = StreamingMode.NO
    aliases: tuple[str, ...] = ()
