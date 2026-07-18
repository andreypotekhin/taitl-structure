from __future__ import annotations

from dataclasses import dataclass

from structure.core.compiler.compileability.streaming_compatibility.model.StreamingSupport import StreamingSupport
from structure.core.compiler.ir.model.AggregatePlan import AggregatePlan
from structure.core.compiler.ir.model.CachePlan import CachePlan
from structure.core.compiler.ir.model.DuplicateRowsPlan import DuplicateRowsPlan
from structure.core.compiler.ir.model.JoinMethod import JoinMethod
from structure.core.compiler.ir.model.JoinPlan import JoinPlan
from structure.core.compiler.ir.model.OperationCapability import OperationCapability
from structure.core.compiler.ir.model.OperationCardinality import OperationCardinality
from structure.core.compiler.ir.model.SelectedRowsPlan import SelectedRowsPlan
from structure.core.compiler.ir.model.WatermarkPlan import WatermarkPlan
from structure.core.dsl.model.expr.Expression import Expression
from structure.core.dsl.model.transforms.StreamingOutputMode import StreamingOutputMode


@dataclass(frozen=True)
class OperationPlan:
    kind: str
    filter: Expression | None = None
    join: JoinPlan | None = None
    aggregate: AggregatePlan | None = None
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
    def filter_operation(predicate: Expression) -> "OperationPlan":
        return OperationPlan(
            kind="filter",
            filter=predicate,
            family="filter",
            capability=OperationCapability(group="expression", name="filter"),
            cardinality=OperationCardinality.ROW_FILTERING,
            streaming=StreamingSupport.COMPATIBLE,
        )

    @staticmethod
    def join_operation(join: JoinPlan) -> "OperationPlan":
        cardinality = {
            JoinMethod.LOOKUP: OperationCardinality.SELECT_ONE,
            JoinMethod.EXISTS: OperationCardinality.ROW_FILTERING,
            JoinMethod.NOT_EXISTS: OperationCardinality.ROW_FILTERING,
            JoinMethod.ROWSET: OperationCardinality.ROW_MULTIPLYING,
            JoinMethod.TEMPORAL_ONE: OperationCardinality.SELECT_ONE,
            JoinMethod.AS_OF_ONE: OperationCardinality.SELECT_ONE,
        }[join.method]
        return OperationPlan(
            kind="join",
            join=join,
            family="join",
            capability=OperationCapability(group="join", name=join.method.value),
            cardinality=cardinality,
            streaming=StreamingSupport.UNKNOWN,
        )

    @staticmethod
    def aggregate_operation(aggregate: AggregatePlan) -> "OperationPlan":
        session_window = any((key.expression.data or {}).get("function") == "session_window" for key in aggregate.keys)
        return OperationPlan(
            kind="aggregate",
            aggregate=aggregate,
            family="aggregate",
            capability=OperationCapability(group="aggregate", name=aggregate.grouping),
            cardinality=OperationCardinality.AGGREGATE,
            streaming=StreamingSupport.BATCH_ONLY,
            streaming_output_modes=(
                (StreamingOutputMode.APPEND,)
                if session_window
                else (StreamingOutputMode.APPEND, StreamingOutputMode.UPDATE)
            ),
        )

    @staticmethod
    def selected_rows_operation(selected_rows: SelectedRowsPlan) -> "OperationPlan":
        return OperationPlan(
            kind="selected_rows",
            selected_rows=selected_rows,
            family="window",
            capability=OperationCapability(group="window", name=f"select_{selected_rows.direction}"),
            cardinality=OperationCardinality.SELECT_ONE,
            streaming=StreamingSupport.BATCH_ONLY,
        )

    @staticmethod
    def drop_duplicates_operation(duplicate_rows: DuplicateRowsPlan | None = None) -> "OperationPlan":
        return OperationPlan(
            kind="drop_duplicates",
            duplicate_rows=duplicate_rows or DuplicateRowsPlan(),
            family="dedupe",
            capability=OperationCapability(group="dedupe", name="drop_duplicates"),
            cardinality=OperationCardinality.ROW_FILTERING,
            streaming=StreamingSupport.BATCH_ONLY,
            streaming_output_modes=(StreamingOutputMode.APPEND,),
        )

    @staticmethod
    def watermark_operation(watermark: WatermarkPlan) -> "OperationPlan":
        return OperationPlan(
            kind="watermark",
            watermark=watermark,
            family="streaming",
            capability=OperationCapability(group="streaming", name="watermark"),
            cardinality=OperationCardinality.ROW_PRESERVING,
            streaming=StreamingSupport.COMPATIBLE,
        )

    @staticmethod
    def cache_operation(cache: CachePlan) -> "OperationPlan":
        return OperationPlan(
            kind="cache",
            cache=cache,
            family="optimization",
            capability=OperationCapability(group="optimization", name="cache"),
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
    ) -> "OperationPlan":
        return OperationPlan(
            kind=kind,
            family=group,
            capability=OperationCapability(group=group, name=name),
            cardinality=cardinality,
            streaming=streaming,
        )
