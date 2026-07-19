from __future__ import annotations

from dataclasses import dataclass

from structure.platform.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkDuplicateRowsRecipe:
    subset: tuple[PySparkExpressionRecipe, ...] = ()
    scope: str | None = None
    within_watermark: bool = False
