from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Any

from structure.app.runtime.schemas.model.TransformSchemas import ResultSchemas


class TransformResult(Mapping[str, Any]):
    _structure_outputs: Mapping[str, Any]
    _structure_single: bool
    schema: ResultSchemas

    def __init__(
        self,
        outputs: Mapping[str, Any],
        *,
        single: bool = False,
        schema: Mapping[str, Any] | None = None,
    ) -> None:
        values = dict(outputs)
        object.__setattr__(self, "_structure_single", single)
        if single:
            if len(values) != 1:
                raise ValueError("single-output TransformResult requires exactly one output")
        object.__setattr__(self, "_structure_outputs", MappingProxyType(values))
        object.__setattr__(self, "schema", ResultSchemas(schema))

    def __getitem__(self, name: str) -> Any:
        return self._structure_outputs[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._structure_outputs)

    def __len__(self) -> int:
        return len(self._structure_outputs)

    def __getattr__(self, name: str) -> Any:
        if name in self._structure_outputs:
            return self._structure_outputs[name]
        raise AttributeError(name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("TransformResult is read-only")

    def as_dict(self) -> dict[str, Any]:
        return dict(self._structure_outputs)

    def _structure_with_schema(self, schema: Mapping[str, Any]) -> TransformResult:
        object.__setattr__(self, "schema", ResultSchemas(schema))
        return self
