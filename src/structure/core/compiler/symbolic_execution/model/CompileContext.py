from __future__ import annotations

from contextvars import Token
from types import TracebackType
from typing import Any

from structure.plugin.api.v1.model.SymbolicContext import (
    SymbolicContext,
    current_symbolic_context,
    install_symbolic_context,
    reset_symbolic_context,
)


class CompileContext:

    def __init__(self, *, step: str, capture_special_exprs: bool = False) -> None:
        self.step = step
        self.capture_special_exprs = capture_special_exprs
        self.filters: list[Any] = []
        self.joins: list[Any] = []
        self.operations: list[Any] = []
        self.aggregate_keys: tuple[tuple[str, Any], ...] | None = None
        self.aggregate_levels: tuple[tuple[str, ...], ...] = ()
        self.aggregate_grouping: str = "group_by"
        self.aggregate_having: Any | None = None
        self.default_project_source: object | None = None
        self.current_scopes: set[str] = set()
        self.relation_scopes: dict[str, object] = {}
        self._token: Token[SymbolicContext | None] | None = None

    def __enter__(self) -> "CompileContext":
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


def current_context() -> SymbolicContext | None:
    return current_symbolic_context()
