from __future__ import annotations

from dataclasses import dataclass

from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe


@dataclass(frozen=True)
class PySparkOperationRecipe:
    kind: str
    filter: PySparkExpressionRecipe | None = None
    join: PySparkJoinRecipe | None = None

    @staticmethod
    def filter_operation(predicate: PySparkExpressionRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="filter", filter=predicate)

    @staticmethod
    def join_operation(join: PySparkJoinRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="join", join=join)
