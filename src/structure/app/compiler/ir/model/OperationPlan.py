from __future__ import annotations

from dataclasses import dataclass

from structure.app.compiler.compileability.streaming_compatibility.model.StreamingSupport import StreamingSupport
from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.model.OperationCapability import OperationCapability
from structure.app.compiler.ir.model.OperationCardinality import OperationCardinality
from structure.app.dsl.model.expr.Expression import Expression


@dataclass(frozen=True)
class OperationPlan:
    kind: str
    filter: Expression | None = None
    join: JoinPlan | None = None
    family: str | None = None
    capability: OperationCapability | None = None
    cardinality: OperationCardinality = OperationCardinality.UNKNOWN
    streaming: StreamingSupport = StreamingSupport.UNKNOWN

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
            JoinMethod.ONE: OperationCardinality.SELECT_ONE,
            JoinMethod.EXISTS: OperationCardinality.ROW_FILTERING,
            JoinMethod.NOT_EXISTS: OperationCardinality.ROW_FILTERING,
            JoinMethod.MANY: OperationCardinality.ROW_MULTIPLYING,
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
