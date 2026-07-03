from __future__ import annotations

from dataclasses import dataclass

from structure.app.target.pyspark.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkSelectedRowsRecipe import PySparkSelectedRowsRecipe


@dataclass(frozen=True)
class PySparkOperationRecipe:
    kind: str
    filter: PySparkExpressionRecipe | None = None
    join: PySparkJoinRecipe | None = None
    aggregate: PySparkAggregateRecipe | None = None
    selected_rows: PySparkSelectedRowsRecipe | None = None

    @staticmethod
    def filter_operation(predicate: PySparkExpressionRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="filter", filter=predicate)

    @staticmethod
    def join_operation(join: PySparkJoinRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="join", join=join)

    @staticmethod
    def aggregate_operation(aggregate: PySparkAggregateRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="aggregate", aggregate=aggregate)

    @staticmethod
    def selected_rows_operation(selected_rows: PySparkSelectedRowsRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="selected_rows", selected_rows=selected_rows)
