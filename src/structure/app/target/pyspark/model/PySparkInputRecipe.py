from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.StreamingMode import StreamingMode
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


@dataclass(frozen=True)
class PySparkInputRecipe:
    name: str
    schema: type[Structure]
    ordinal: int
    validation: PySparkValidationRecipe
    streaming: StreamingMode = StreamingMode.NO
    aliases: tuple[str, ...] = ()
