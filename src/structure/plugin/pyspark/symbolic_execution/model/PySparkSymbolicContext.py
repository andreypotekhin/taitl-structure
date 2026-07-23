from __future__ import annotations

from contextvars import Token
from types import TracebackType
from typing import TYPE_CHECKING, Any, cast

from structure.plugin.api.v1.model.SymbolicContext import (
    SymbolicContext,
    current_symbolic_context,
    install_symbolic_context,
    reset_symbolic_context,
)

if TYPE_CHECKING:
    from structure.plugin.pyspark.dsl.aggregation.AggregatePlan import AggregatePlan
    from structure.plugin.pyspark.dsl.aggregation.ProjectAssignment import ProjectAssignment
    from structure.plugin.pyspark.dsl.Expression import Expression
    from structure.plugin.pyspark.dsl.joins.JoinPlan import JoinPlan
    from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan


class PySparkSymbolicContext:

    def __init__(self, *, step: str, capture_special_exprs: bool = False) -> None:
        self.step = step
        self.capture_special_exprs = capture_special_exprs
        self.filters: list[Expression] = []
        self.joins: list[JoinPlan] = []
        self.operations: list[OperationPlan] = []
        self.aggregate_keys: tuple[tuple[str, Expression], ...] | None = None
        self.aggregate_levels: tuple[tuple[str, ...], ...] = ()
        self.aggregate_grouping = "group_by"
        self.aggregate_having: Expression | None = None
        self.projection: tuple[ProjectAssignment, ...] = ()
        self.aggregate: AggregatePlan | None = None
        self.results: tuple[object, ...] = ()
        self.default_project_source: object | None = None
        self.current_scopes: set[str] = set()
        self.relation_scopes: dict[str, object] = {}
        self._token: Token[SymbolicContext | None] | None = None

    def __enter__(self) -> PySparkSymbolicContext:
        self._token = install_symbolic_context(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._token is not None:
            reset_symbolic_context(self._token)

    def register_current_scope(self, scope: str) -> None:
        self.current_scopes.add(scope)

    def register_relation_scope(self, scope: str, relation: object) -> object:
        existing = self.relation_scopes.get(scope)
        if existing is not None:
            return existing
        self.relation_scopes[scope] = relation
        return relation

    def record_aggregate(self, aggregate: object) -> None:
        from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan

        self.operations.append(OperationPlan.aggregate_operation(cast(Any, aggregate)))

    def input_scope(self, *, name: str, schema: object) -> object:
        from structure.plugin.pyspark.dsl.InputScope import InputScope

        return InputScope(name=name, schema=cast(type, schema))

    def project(self, source: object, *, target: object) -> object:
        from structure.plugin.pyspark.dsl.body import project

        return project(source, cast(Any, target))

    def special(self, function, *, type: str, return_type: object | None, nullable: bool, args, kwargs):
        from structure.plugin.pyspark.dsl.SpecialFunction import SpecialFunction

        return SpecialFunction(function, type=type, return_type=return_type, nullable=nullable)(*args, **kwargs)


def current_pyspark_context() -> PySparkSymbolicContext | None:
    context = current_symbolic_context()
    return context if isinstance(context, PySparkSymbolicContext) else None
