from __future__ import annotations

from typing import Callable

from structure.app.dsl.model.expr.expressions import literal


class ExprFunction:

    def __init__(self, function: Callable) -> None:
        self.function = function
        self.__name__ = function.__name__

    def __call__(self, *args, **kwargs):
        return literal(self.function(*args, **kwargs))

    def __get__(self, instance: object, owner: type | None = None):
        return self.__call__
