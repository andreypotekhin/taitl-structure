from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any


class ResultSchemas(Mapping[str, Any]):
    _schemas: Mapping[str, Any]

    def __init__(self, schemas: Mapping[str, Any] | None = None) -> None:
        object.__setattr__(self, "_schemas", MappingProxyType(dict(schemas or {})))

    def __getitem__(self, name: str) -> Any:
        return self._schemas[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._schemas)

    def __len__(self) -> int:
        return len(self._schemas)

    def __getattr__(self, name: str) -> Any:
        if name in self._schemas:
            return self._schemas[name]
        raise AttributeError(name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("ResultSchemas is read-only")

    def as_dict(self) -> dict[str, Any]:
        return dict(self._schemas)


@dataclass(frozen=True)
class TransformSchemas:
    inputs: Mapping[str, Any]
    steps: Mapping[str, Any]
    outputs: Mapping[str, Any]

    @property
    def output(self) -> Any:
        if len(self.outputs) != 1:
            names = ", ".join(self.outputs)
            raise AttributeError(f"Transform has multiple outputs: {names}. Use result.schema[...] after run(session).")
        return next(iter(self.outputs.values()))
