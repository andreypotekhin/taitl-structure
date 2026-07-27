from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from structure.plugin.pyspark.dsl.operations.CachePlan import CachePlan
from structure.plugin.pyspark.dsl.operations.DuplicateRowsPlan import DuplicateRowsPlan
from structure.plugin.pyspark.dsl.operations.ExactlyOnePlan import ExactlyOnePlan
from structure.plugin.pyspark.dsl.operations.OperationCapability import OperationCapability
from structure.plugin.pyspark.dsl.operations.OperationCardinality import OperationCardinality
from structure.plugin.pyspark.dsl.operations.PosexplodeStructPlan import PosexplodeStructPlan
from structure.plugin.pyspark.dsl.operations.RelationAliasPlan import RelationAliasPlan
from structure.plugin.pyspark.dsl.operations.RelationAssertionPlan import RelationAssertionPlan
from structure.plugin.pyspark.dsl.operations.RelationBoundPlan import RelationBoundPlan
from structure.plugin.pyspark.dsl.operations.RelationOrderPlan import RelationOrderPlan
from structure.plugin.pyspark.dsl.operations.RelationPrioritySelectionPlan import RelationPrioritySelectionPlan
from structure.plugin.pyspark.dsl.operations.RelationSetPlan import RelationSetPlan
from structure.plugin.pyspark.dsl.operations.SelectedRowsPlan import SelectedRowsPlan
from structure.plugin.pyspark.dsl.operations.StreamingOutputMode import StreamingOutputMode
from structure.plugin.pyspark.dsl.operations.StreamingSupport import StreamingSupport
from structure.plugin.pyspark.dsl.operations.WatermarkPlan import WatermarkPlan


@dataclass(frozen=True)
class OperationPlan:
    kind: str
    filter: Any | None = None
    join: Any | None = None
    aggregate: Any | None = None
    selected_rows: SelectedRowsPlan | None = None
    duplicate_rows: DuplicateRowsPlan | None = None
    exactly_one: ExactlyOnePlan | None = None
    posexplode_struct: PosexplodeStructPlan | None = None
    relation_alias: RelationAliasPlan | None = None
    relation_assertion: RelationAssertionPlan | None = None
    relation_order: RelationOrderPlan | None = None
    relation_priority_selection: RelationPrioritySelectionPlan | None = None
    relation_bound: RelationBoundPlan | None = None
    relation_set: RelationSetPlan | None = None
    watermark: WatermarkPlan | None = None
    cache: CachePlan | None = None
    family: str | None = None
    capability: OperationCapability | None = None
    cardinality: OperationCardinality = OperationCardinality.UNKNOWN
    streaming: StreamingSupport = StreamingSupport.UNKNOWN
    streaming_output_modes: tuple[StreamingOutputMode, ...] = ()

    @staticmethod
    def filter_operation(predicate: Any) -> OperationPlan:
        return OperationPlan(
            "filter",
            filter=predicate,
            family="filter",
            capability=OperationCapability("expression", "filter"),
            cardinality=OperationCardinality.ROW_FILTERING,
            streaming=StreamingSupport.COMPATIBLE,
        )

    @staticmethod
    def join_operation(join: Any) -> OperationPlan:
        cardinality = {
            "lookup_join": OperationCardinality.SELECT_ONE,
            "exists": OperationCardinality.ROW_FILTERING,
            "not_exists": OperationCardinality.ROW_FILTERING,
            "rowset_join": OperationCardinality.ROW_MULTIPLYING,
            "temporal_one": OperationCardinality.SELECT_ONE,
            "as_of_one": OperationCardinality.SELECT_ONE,
        }[join.method.value]
        return OperationPlan(
            "join",
            join=join,
            family="join",
            capability=OperationCapability("join", join.method.value),
            cardinality=cardinality,
            streaming=StreamingSupport.UNKNOWN,
        )

    @staticmethod
    def aggregate_operation(aggregate: Any) -> OperationPlan:
        session_window = any((key.expression.data or {}).get("function") == "session_window" for key in aggregate.keys)
        modes = (
            (StreamingOutputMode.APPEND,)
            if session_window
            else (StreamingOutputMode.APPEND, StreamingOutputMode.UPDATE)
        )
        return OperationPlan(
            "aggregate",
            aggregate=aggregate,
            family="aggregate",
            capability=OperationCapability("aggregate", aggregate.grouping),
            cardinality=OperationCardinality.AGGREGATE,
            streaming=StreamingSupport.BATCH_ONLY,
            streaming_output_modes=modes,
        )

    @staticmethod
    def selected_rows_operation(selected_rows: SelectedRowsPlan) -> OperationPlan:
        return OperationPlan(
            "selected_rows",
            selected_rows=selected_rows,
            family="window",
            capability=OperationCapability("window", f"select_{selected_rows.direction}"),
            cardinality=OperationCardinality.SELECT_ONE,
            streaming=StreamingSupport.BATCH_ONLY,
        )

    @staticmethod
    def drop_duplicates_operation(duplicate_rows: DuplicateRowsPlan | None = None) -> OperationPlan:
        return OperationPlan(
            "drop_duplicates",
            duplicate_rows=duplicate_rows or DuplicateRowsPlan(),
            family="dedupe",
            capability=OperationCapability("dedupe", "drop_duplicates"),
            cardinality=OperationCardinality.ROW_FILTERING,
            streaming=StreamingSupport.BATCH_ONLY,
            streaming_output_modes=(StreamingOutputMode.APPEND,),
        )

    @staticmethod
    def exactly_one_operation(exactly_one: ExactlyOnePlan) -> OperationPlan:
        return OperationPlan(
            "exactly_one",
            exactly_one=exactly_one,
            family="relation",
            capability=OperationCapability("relation", "exactly_one"),
            cardinality=OperationCardinality.ROW_PRESERVING,
            streaming=StreamingSupport.BATCH_ONLY,
        )

    @staticmethod
    def posexplode_struct_operation(posexplode_struct: PosexplodeStructPlan) -> OperationPlan:
        return OperationPlan(
            "posexplode_struct",
            posexplode_struct=posexplode_struct,
            family="generator",
            capability=OperationCapability("generator", "posexplode_struct"),
            cardinality=OperationCardinality.ROW_MULTIPLYING,
            streaming=StreamingSupport.BATCH_ONLY,
        )

    @staticmethod
    def relation_alias_operation(relation_alias: RelationAliasPlan) -> OperationPlan:
        return OperationPlan(
            "relation_alias",
            relation_alias=relation_alias,
            family="relation",
            capability=OperationCapability("relation", "relation_alias"),
            cardinality=OperationCardinality.ROW_PRESERVING,
            streaming=StreamingSupport.COMPATIBLE,
        )

    @staticmethod
    def relation_assertion_operation(relation_assertion: RelationAssertionPlan) -> OperationPlan:
        return OperationPlan(
            relation_assertion.operation,
            relation_assertion=relation_assertion,
            family="relation",
            capability=OperationCapability("relation", relation_assertion.operation),
            cardinality=OperationCardinality.ROW_PRESERVING,
            streaming=StreamingSupport.BATCH_ONLY,
        )

    @staticmethod
    def relation_order_operation(relation_order: RelationOrderPlan) -> OperationPlan:
        return OperationPlan(
            "order_by",
            relation_order=relation_order,
            family="relation",
            capability=OperationCapability("relation", "order_by"),
            cardinality=OperationCardinality.ROW_PRESERVING,
            streaming=StreamingSupport.BATCH_ONLY,
        )

    @staticmethod
    def relation_bound_operation(kind: str, relation_bound: RelationBoundPlan) -> OperationPlan:
        return OperationPlan(
            kind,
            relation_bound=relation_bound,
            family="relation",
            capability=OperationCapability("relation", kind),
            cardinality=OperationCardinality.ROW_FILTERING,
            streaming=StreamingSupport.BATCH_ONLY,
        )

    @staticmethod
    def relation_priority_selection_operation(selection: RelationPrioritySelectionPlan) -> OperationPlan:
        return OperationPlan(
            "select_first_qualified",
            relation_priority_selection=selection,
            family="relation",
            capability=OperationCapability("relation", "select_first_qualified"),
            cardinality=OperationCardinality.SELECT_ONE,
            streaming=StreamingSupport.BATCH_ONLY,
        )

    @staticmethod
    def relation_set_operation(relation_set: RelationSetPlan) -> OperationPlan:
        cardinality = (
            OperationCardinality.ROW_MULTIPLYING
            if relation_set.operation in {"union_all", "union_by_name"}
            else OperationCardinality.ROW_FILTERING
        )
        return OperationPlan(
            relation_set.operation,
            relation_set=relation_set,
            family="set",
            capability=OperationCapability("set", relation_set.operation),
            cardinality=cardinality,
            streaming=StreamingSupport.BATCH_ONLY,
        )

    @staticmethod
    def watermark_operation(watermark: WatermarkPlan) -> OperationPlan:
        return OperationPlan(
            "watermark",
            watermark=watermark,
            family="streaming",
            capability=OperationCapability("streaming", "watermark"),
            cardinality=OperationCardinality.ROW_PRESERVING,
            streaming=StreamingSupport.COMPATIBLE,
        )

    @staticmethod
    def cache_operation(cache: CachePlan) -> OperationPlan:
        return OperationPlan(
            "cache",
            cache=cache,
            family="optimization",
            capability=OperationCapability("optimization", "cache"),
            cardinality=OperationCardinality.ROW_PRESERVING,
            streaming=StreamingSupport.BATCH_ONLY,
        )

    @staticmethod
    def reserved_operation(
        kind: str,
        *,
        group: str,
        name: str,
        cardinality: OperationCardinality = OperationCardinality.UNKNOWN,
        streaming: StreamingSupport = StreamingSupport.UNKNOWN,
    ) -> OperationPlan:
        return OperationPlan(
            kind,
            family=group,
            capability=OperationCapability(group, name),
            cardinality=cardinality,
            streaming=streaming,
        )
