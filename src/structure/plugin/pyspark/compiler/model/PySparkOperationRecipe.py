from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.plugin.pyspark.compiler.model.PySparkCacheRecipe import PySparkCacheRecipe
from structure.plugin.pyspark.compiler.model.PySparkDuplicateRowsRecipe import PySparkDuplicateRowsRecipe
from structure.plugin.pyspark.compiler.model.PySparkExactlyOneRecipe import PySparkExactlyOneRecipe
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.compiler.model.PySparkPosexplodeStructRecipe import PySparkPosexplodeStructRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationAliasRecipe import PySparkRelationAliasRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationAssertionRecipe import PySparkRelationAssertionRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationBoundRecipe import PySparkRelationBoundRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationOrderRecipe import PySparkRelationOrderRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationPrioritySelectionRecipe import (
    PySparkRelationPrioritySelectionRecipe,
)
from structure.plugin.pyspark.compiler.model.PySparkRelationSetRecipe import PySparkRelationSetRecipe
from structure.plugin.pyspark.compiler.model.PySparkSelectedRowsRecipe import PySparkSelectedRowsRecipe
from structure.plugin.pyspark.compiler.model.PySparkWatermarkRecipe import PySparkWatermarkRecipe
from structure.plugin.pyspark.dsl.operations import StreamingOutputMode


@dataclass(frozen=True)
class PySparkOperationRecipe:
    kind: str
    filter: PySparkExpressionRecipe | None = None
    join: PySparkJoinRecipe | None = None
    aggregate: PySparkAggregateRecipe | None = None
    selected_rows: PySparkSelectedRowsRecipe | None = None
    duplicate_rows: PySparkDuplicateRowsRecipe | None = None
    exactly_one: PySparkExactlyOneRecipe | None = None
    posexplode_struct: PySparkPosexplodeStructRecipe | None = None
    relation_alias: PySparkRelationAliasRecipe | None = None
    relation_assertion: PySparkRelationAssertionRecipe | None = None
    relation_order: PySparkRelationOrderRecipe | None = None
    relation_priority_selection: PySparkRelationPrioritySelectionRecipe | None = None
    relation_bound: PySparkRelationBoundRecipe | None = None
    relation_set: PySparkRelationSetRecipe | None = None
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
    def exactly_one_operation(exactly_one: PySparkExactlyOneRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="exactly_one", exactly_one=exactly_one)

    @staticmethod
    def posexplode_struct_operation(posexplode_struct: PySparkPosexplodeStructRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="posexplode_struct", posexplode_struct=posexplode_struct)

    @staticmethod
    def relation_alias_operation(relation_alias: PySparkRelationAliasRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="relation_alias", relation_alias=relation_alias)

    @staticmethod
    def relation_assertion_operation(relation_assertion: PySparkRelationAssertionRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(
            kind=relation_assertion.operation,
            relation_assertion=relation_assertion,
        )

    @staticmethod
    def relation_order_operation(relation_order: PySparkRelationOrderRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="order_by", relation_order=relation_order)

    @staticmethod
    def relation_bound_operation(kind: str, relation_bound: PySparkRelationBoundRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind=kind, relation_bound=relation_bound)

    @staticmethod
    def relation_priority_selection_operation(
        selection: PySparkRelationPrioritySelectionRecipe,
    ) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(
            kind="select_first_qualified",
            relation_priority_selection=selection,
        )

    @staticmethod
    def relation_set_operation(relation_set: PySparkRelationSetRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(
            kind=relation_set.operation,
            relation_set=relation_set,
        )

    @staticmethod
    def watermark_operation(watermark: PySparkWatermarkRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="watermark", watermark=watermark)

    @staticmethod
    def cache_operation(cache: PySparkCacheRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="cache", cache=cache)
