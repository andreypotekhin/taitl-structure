from __future__ import annotations

from contextvars import ContextVar, Token
from types import TracebackType

from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.dsl.model.expr.Expression import Expression

_current: ContextVar["CompileContext | None"] = ContextVar("structure_compile_context", default=None)


class CompileContext:

    def __init__(self, *, step: str) -> None:
        self.step = step
        self.filters: list[Expression] = []
        self.joins: list[JoinPlan] = []
        self.operations: list[OperationPlan] = []
        self.default_project_source: object | None = None
        self.current_scopes: set[str] = set()
        self.relation_scopes: dict[str, object] = {}
        self._token: Token[CompileContext | None] | None = None

    def __enter__(self) -> "CompileContext":
        self._token = _current.set(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._token is not None:
            _current.reset(self._token)

    def register_current_scope(self, scope: str) -> None:
        self.current_scopes.add(scope)

    def register_relation_scope(self, scope: str, relation: object) -> object:
        existing = self.relation_scopes.get(scope)
        if existing is not None:
            return existing
        self.relation_scopes[scope] = relation
        return relation


def current_context() -> CompileContext | None:
    return _current.get()
