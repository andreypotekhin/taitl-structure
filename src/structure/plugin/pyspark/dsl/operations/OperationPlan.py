from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from structure.plugin.pyspark.dsl.operations.CachePlan import CachePlan
from structure.plugin.pyspark.dsl.operations.DuplicateRowsPlan import DuplicateRowsPlan
from structure.plugin.pyspark.dsl.operations.OperationCapability import OperationCapability
from structure.plugin.pyspark.dsl.operations.OperationCardinality import OperationCardinality
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
    watermark: WatermarkPlan | None = None
    cache: CachePlan | None = None
    family: str | None = None
    capability: OperationCapability | None = None
    cardinality: OperationCardinality = OperationCardinality.UNKNOWN
    streaming: StreamingSupport = StreamingSupport.UNKNOWN
    streaming_output_modes: tuple[StreamingOutputMode, ...] = ()

    @staticmethod
    def filter_operation(predicate: Any) -> OperationPlan:
        return OperationPlan("filter", filter=predicate, family="filter", capability=OperationCapability("expression", "filter"), cardinality=OperationCardinality.ROW_FILTERING, streaming=StreamingSupport.COMPATIBLE)

    @staticmethod
    def join_operation(join: Any) -> OperationPlan:
        cardinality = {
            "lookup_join": OperationCardinality.SELECT_ONE, "exists": OperationCardinality.ROW_FILTERING,
            "not_exists": OperationCardinality.ROW_FILTERING, "rowset_join": OperationCardinality.ROW_MULTIPLYING,
            "temporal_one": OperationCardinality.SELECT_ONE, "as_of_one": OperationCardinality.SELECT_ONE,
        }[join.method.value]
        return OperationPlan("join", join=join, family="join", capability=OperationCapability("join", join.method.value), cardinality=cardinality, streaming=StreamingSupport.UNKNOWN)

    @staticmethod
    def aggregate_operation(aggregate: Any) -> OperationPlan:
        session_window = any((key.expression.data or {}).get("function") == "session_window" for key in aggregate.keys)
        modes = (StreamingOutputMode.APPEND,) if session_window else (StreamingOutputMode.APPEND, StreamingOutputMode.UPDATE)
        return OperationPlan("aggregate", aggregate=aggregate, family="aggregate", capability=OperationCapability("aggregate", aggregate.grouping), cardinality=OperationCardinality.AGGREGATE, streaming=StreamingSupport.BATCH_ONLY, streaming_output_modes=modes)

    @staticmethod
    def selected_rows_operation(selected_rows: SelectedRowsPlan) -> OperationPlan:
        return OperationPlan("selected_rows", selected_rows=selected_rows, family="window", capability=OperationCapability("window", f"select_{selected_rows.direction}"), cardinality=OperationCardinality.SELECT_ONE, streaming=StreamingSupport.BATCH_ONLY)

    @staticmethod
    def drop_duplicates_operation(duplicate_rows: DuplicateRowsPlan | None = None) -> OperationPlan:
        return OperationPlan("drop_duplicates", duplicate_rows=duplicate_rows or DuplicateRowsPlan(), family="dedupe", capability=OperationCapability("dedupe", "drop_duplicates"), cardinality=OperationCardinality.ROW_FILTERING, streaming=StreamingSupport.BATCH_ONLY, streaming_output_modes=(StreamingOutputMode.APPEND,))

    @staticmethod
    def watermark_operation(watermark: WatermarkPlan) -> OperationPlan:
        return OperationPlan("watermark", watermark=watermark, family="streaming", capability=OperationCapability("streaming", "watermark"), cardinality=OperationCardinality.ROW_PRESERVING, streaming=StreamingSupport.COMPATIBLE)

    @staticmethod
    def cache_operation(cache: CachePlan) -> OperationPlan:
        return OperationPlan("cache", cache=cache, family="optimization", capability=OperationCapability("optimization", "cache"), cardinality=OperationCardinality.ROW_PRESERVING, streaming=StreamingSupport.BATCH_ONLY)

    @staticmethod
    def reserved_operation(kind: str, *, group: str, name: str, cardinality: OperationCardinality = OperationCardinality.UNKNOWN, streaming: StreamingSupport = StreamingSupport.UNKNOWN) -> OperationPlan:
        return OperationPlan(kind, family=group, capability=OperationCapability(group, name), cardinality=cardinality, streaming=streaming)
