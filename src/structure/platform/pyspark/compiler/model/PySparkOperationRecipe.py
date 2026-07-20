from __future__ import annotations

from dataclasses import dataclass

from structure.platform.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.platform.pyspark.compiler.model.PySparkCacheRecipe import PySparkCacheRecipe
from structure.platform.pyspark.compiler.model.PySparkDuplicateRowsRecipe import PySparkDuplicateRowsRecipe
from structure.platform.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.platform.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.platform.pyspark.compiler.model.PySparkSelectedRowsRecipe import PySparkSelectedRowsRecipe
from structure.platform.pyspark.compiler.model.PySparkWatermarkRecipe import PySparkWatermarkRecipe
from structure.platform.pyspark.dsl.operations import StreamingOutputMode


@dataclass(frozen=True)
class PySparkOperationRecipe:
    kind: str
    filter: PySparkExpressionRecipe | None = None
    join: PySparkJoinRecipe | None = None
    aggregate: PySparkAggregateRecipe | None = None
    selected_rows: PySparkSelectedRowsRecipe | None = None
    duplicate_rows: PySparkDuplicateRowsRecipe | None = None
    watermark: PySparkWatermarkRecipe | None = None
    cache: PySparkCacheRecipe | None = None
    streaming_output_modes: tuple[StreamingOutputMode, ...] = ()

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

    @staticmethod
    def drop_duplicates_operation(duplicate_rows: PySparkDuplicateRowsRecipe | None = None) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(
            kind="drop_duplicates",
            duplicate_rows=duplicate_rows or PySparkDuplicateRowsRecipe(),
        )

    @staticmethod
    def watermark_operation(watermark: PySparkWatermarkRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="watermark", watermark=watermark)

    @staticmethod
    def cache_operation(cache: PySparkCacheRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="cache", cache=cache)
