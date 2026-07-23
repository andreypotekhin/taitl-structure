from __future__ import annotations

from contextvars import ContextVar, Token
from types import TracebackType
from typing import Any, Protocol


class SymbolicContext(Protocol):
    step: str
    capture_special_exprs: bool
    filters: list[Any]
    joins: list[Any]
    operations: list[Any]
    aggregate_keys: tuple[tuple[str, Any], ...] | None
    aggregate_requested: bool
    aggregate_levels: tuple[tuple[str, ...], ...]
    aggregate_grouping: str
    aggregate_having: Any | None
    default_project_source: object | None
    current_scopes: set[str]
    relation_scopes: dict[str, object]

    def __enter__(self) -> SymbolicContext: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    def register_current_scope(self, scope: str) -> None: ...

    def register_relation_scope(self, scope: str, relation: object) -> object: ...


_current: ContextVar[SymbolicContext | None] = ContextVar("structure_symbolic_context", default=None)


def current_symbolic_context() -> SymbolicContext | None:
    return _current.get()


def install_symbolic_context(context: SymbolicContext) -> Token[SymbolicContext | None]:
    return _current.set(context)


def reset_symbolic_context(token: Token[SymbolicContext | None]) -> None:
    _current.reset(token)
