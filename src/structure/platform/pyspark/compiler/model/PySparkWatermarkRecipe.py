from __future__ import annotations

from dataclasses import dataclass

from structure.platform.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


@dataclass(frozen=True)
class PySparkWatermarkRecipe:
    expression: PySparkExpressionRecipe
    delay: str

    @property
    def scope(self) -> str:
        return str(self.expression.data.get("scope", ""))

    @property
    def column(self) -> str:
        return str(self.expression.data.get("field", ""))
