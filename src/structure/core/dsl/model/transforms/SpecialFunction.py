"""Target-neutral wrapper created by ``@special``."""

from __future__ import annotations

from typing import Callable

from structure.plugin.api.v1.model import current_symbolic_context


class IgnoredCompilerCode(TypeError):
    """Signal that deliberately non-compiler code was reached symbolically."""


class SpecialFunction:
    """A helper function with plugin-visible symbolic behavior.

    Outside compilation, the wrapped function behaves like ordinary Python.
    During compilation, calls are delegated to the active plugin so a backend
    such as PySpark can expand expressions or create UDF nodes. ``ignore``
    helpers are rejected before delegation because they are outside the
    compiler-visible contract.
    """

    def __init__(
        self,
        function: Callable,
        *,
        type: str,
        return_type: object | None = None,
        nullable: bool = True,
    ) -> None:
        self.function = function
        self.type = type
        self.return_type = return_type
        self.nullable = nullable
        self.__name__ = function.__name__
        self.__qualname__ = function.__qualname__
        self.__module__ = function.__module__

    def __call__(self, *args, **kwargs):
        """Call normally or delegate symbolic behavior to the active plugin."""
        context = current_symbolic_context()
        if context is None:
            return self.function(*args, **kwargs)
        if self.type == "ignore":
            raise IgnoredCompilerCode(
                f"{self.function.__qualname__} is marked @special(type=\"ignore\") and cannot be used in "
                "compiler-visible logic"
            )
        special = getattr(context, "special", None)
        if not callable(special):
            raise TypeError("The selected plugin does not support @special(...) symbolic calls")
        return special(self.function, type=self.type, return_type=self.return_type, nullable=self.nullable, args=args, kwargs=kwargs)

    def __get__(self, instance: object, owner: type | None = None):
        """Bind decorated methods without hiding the wrapper on the class."""
        if instance is None or self.type != "ignore":
            return self if instance is None else self.__call__
        return lambda *args, **kwargs: self(instance, *args, **kwargs)
