from __future__ import annotations

from dataclasses import dataclass

from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkDuplicateRowsRecipe:
    subset: tuple[PySparkExpressionRecipe, ...] = ()
