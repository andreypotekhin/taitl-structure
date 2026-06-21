from __future__ import annotations

from contextvars import ContextVar, Token
from types import TracebackType

from structure.app.dsl.logic.model.expr.Expression import Expression

_current: ContextVar["CompileContext | None"] = ContextVar("structure_compile_context", default=None)


class CompileContext:

    def __init__(self, *, step: str) -> None:
        self.step = step
        self.filters: list[Expression] = []
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


def current_context() -> CompileContext | None:
    return _current.get()
