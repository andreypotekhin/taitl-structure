from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceModulePath:
    """Validated dotted Python module identity and its relative path form."""

    parts: tuple[str, ...]

    @classmethod
    def from_module(cls, module: str) -> "SourceModulePath":
        if not isinstance(module, str) or not module:
            raise ValueError(f"Python module name must be a non-empty string: {module!r}")
        parts = tuple(module.split("."))
        if any(not part or not part.isidentifier() or part == "__init__" for part in parts):
            raise ValueError(f"Invalid Python module name: {module!r}")
        return cls(parts)

    @property
    def module(self) -> str:
        return ".".join(self.parts)

    @property
    def path(self) -> str:
        return "/".join(self.parts)
