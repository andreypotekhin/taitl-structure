from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.pyspark.compiler.model.PySparkAggregateRecipe import PySparkAggregateRecipe
from structure.plugin.pyspark.compiler.model.PySparkCacheRecipe import PySparkCacheRecipe
from structure.plugin.pyspark.compiler.model.PySparkDuplicateRowsRecipe import PySparkDuplicateRowsRecipe
from structure.plugin.pyspark.compiler.model.PySparkExactlyOneRecipe import PySparkExactlyOneRecipe
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.plugin.pyspark.compiler.model.PySparkOrderedTimelineScanRecipe import PySparkOrderedTimelineScanRecipe
from structure.plugin.pyspark.compiler.model.PySparkPosexplodeStructRecipe import PySparkPosexplodeStructRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationAliasRecipe import PySparkRelationAliasRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationAssertionRecipe import PySparkRelationAssertionRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationBoundRecipe import PySparkRelationBoundRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationHierarchyClosureRecipe import (
    PySparkRelationHierarchyClosureRecipe,
)
from structure.plugin.pyspark.compiler.model.PySparkRelationHierarchyFallbackRecipe import (
    PySparkRelationHierarchyFallbackRecipe,
)
from structure.plugin.pyspark.compiler.model.PySparkRelationOrderRecipe import PySparkRelationOrderRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationPrioritySelectionRecipe import (
    PySparkRelationPrioritySelectionRecipe,
)
from structure.plugin.pyspark.compiler.model.PySparkRelationSampleRecipe import PySparkRelationSampleRecipe
from structure.plugin.pyspark.compiler.model.PySparkRelationSetRecipe import PySparkRelationSetRecipe
from structure.plugin.pyspark.compiler.model.PySparkScalarGeneratorRecipe import PySparkScalarGeneratorRecipe
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
    scalar_generator: PySparkScalarGeneratorRecipe | None = None
    ordered_timeline_scan: PySparkOrderedTimelineScanRecipe | None = None
    relation_alias: PySparkRelationAliasRecipe | None = None
    relation_assertion: PySparkRelationAssertionRecipe | None = None
    relation_hierarchy_closure: PySparkRelationHierarchyClosureRecipe | None = None
    relation_hierarchy_fallback: PySparkRelationHierarchyFallbackRecipe | None = None
    relation_order: PySparkRelationOrderRecipe | None = None
    relation_priority_selection: PySparkRelationPrioritySelectionRecipe | None = None
    relation_bound: PySparkRelationBoundRecipe | None = None
    relation_sample: PySparkRelationSampleRecipe | None = None
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
    def posexplode_outer_struct_operation(
        posexplode_outer_struct: PySparkPosexplodeStructRecipe,
    ) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="posexplode_outer_struct", posexplode_struct=posexplode_outer_struct)

    @staticmethod
    def explode_struct_operation(explode_struct: PySparkPosexplodeStructRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="explode_struct", posexplode_struct=explode_struct)

    @staticmethod
    def explode_outer_struct_operation(
        explode_outer_struct: PySparkPosexplodeStructRecipe,
    ) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="explode_outer_struct", posexplode_struct=explode_outer_struct)

    @staticmethod
    def inline_struct_operation(inline_struct: PySparkPosexplodeStructRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="inline_struct", posexplode_struct=inline_struct)

    @staticmethod
    def inline_outer_struct_operation(inline_outer_struct: PySparkPosexplodeStructRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="inline_outer_struct", posexplode_struct=inline_outer_struct)

    @staticmethod
    def variant_explode_operation(variant_explode: PySparkPosexplodeStructRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="variant_explode", posexplode_struct=variant_explode)

    @staticmethod
    def variant_explode_outer_operation(
        variant_explode_outer: PySparkPosexplodeStructRecipe,
    ) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="variant_explode_outer", posexplode_struct=variant_explode_outer)

    @staticmethod
    def explode_array_operation(generator: PySparkScalarGeneratorRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="explode_array", scalar_generator=generator)

    @staticmethod
    def explode_outer_array_operation(generator: PySparkScalarGeneratorRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="explode_outer_array", scalar_generator=generator)

    @staticmethod
    def posexplode_array_operation(generator: PySparkScalarGeneratorRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="posexplode_array", scalar_generator=generator)

    @staticmethod
    def posexplode_outer_array_operation(generator: PySparkScalarGeneratorRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="posexplode_outer_array", scalar_generator=generator)

    @staticmethod
    def ordered_timeline_scan_operation(scan: PySparkOrderedTimelineScanRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="ordered_timeline_scan", ordered_timeline_scan=scan)

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
    def relation_sample_operation(relation_sample: PySparkRelationSampleRecipe) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="sample", relation_sample=relation_sample)

    @staticmethod
    def relation_priority_selection_operation(
        selection: PySparkRelationPrioritySelectionRecipe,
    ) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(
            kind="select_first_qualified",
            relation_priority_selection=selection,
        )

    @staticmethod
    def relation_hierarchy_closure_operation(
        closure: PySparkRelationHierarchyClosureRecipe,
    ) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="hierarchy_closure", relation_hierarchy_closure=closure)

    @staticmethod
    def relation_hierarchy_fallback_operation(
        fallback: PySparkRelationHierarchyFallbackRecipe,
    ) -> "PySparkOperationRecipe":
        return PySparkOperationRecipe(kind="hierarchy_fallbacks", relation_hierarchy_fallback=fallback)

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
