"""Public signals and guards for Python code excluded from symbolic compilation."""

from __future__ import annotations

from typing import Any, cast

from structure.plugin.api.v1.model.SymbolicContext import current_symbolic_context


class IgnoredCompilerCode(TypeError):
    """Signal that deliberately excluded Python code was reached symbolically."""


class OpaqueCompilerCode(TypeError):
    """Signal that opaque runtime Python code was called during symbolic compilation."""


def guard_excluded_class(cls: type, *, mode: str, role: str | None = None) -> None:
    """Reject access to callable public members while a role class is compiled."""
    original = getattr(cls, "__getattribute__", object.__getattribute__)
    if getattr(original, "_structure_excluded_guard", False):
        return

    def guarded(instance, name):
        value = original(instance, name)
        if current_symbolic_context() is not None and not name.startswith("_") and callable(value):
            error_type = OpaqueCompilerCode if mode == "opaque" else IgnoredCompilerCode
            marker = role or f'@special(type="{mode}")'
            raise error_type(
                f"{cls.__qualname__}.{name} is excluded by {marker} and cannot be used in compiler-visible logic"
            )
        return value

    setattr(guarded, "_structure_excluded_guard", True)
    setattr(cls, "__getattribute__", cast(Any, guarded))
