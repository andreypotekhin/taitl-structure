from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType


class ResultSchemas(Mapping[str, object]):
    _schemas: Mapping[str, object]

    def __init__(self, schemas: Mapping[str, object] | None = None) -> None:
        object.__setattr__(self, "_schemas", MappingProxyType(dict(schemas or {})))

    def __getitem__(self, name: str) -> object:
        return self._schemas[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._schemas)

    def __len__(self) -> int:
        return len(self._schemas)

    def __getattr__(self, name: str) -> object:
        if name in self._schemas:
            return self._schemas[name]
        raise AttributeError(name)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("ResultSchemas is read-only")

    def as_dict(self) -> dict[str, object]:
        return dict(self._schemas)


@dataclass(frozen=True)
class TransformSchemas:
    inputs: Mapping[str, object]
    steps: Mapping[str, object]
    outputs: Mapping[str, object]

    @property
    def output(self) -> object:
        if len(self.outputs) != 1:
            names = ", ".join(self.outputs)
            raise AttributeError(f"Transform has multiple outputs: {names}. Use result.schema[...] after run(session).")
        return next(iter(self.outputs.values()))
