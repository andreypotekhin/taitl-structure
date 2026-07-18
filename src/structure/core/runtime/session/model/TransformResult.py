from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Any

from structure.core.runtime.schemas.model.TransformSchemas import ResultSchemas


class TransformResult(Mapping[str, Any]):
    _structure_outputs: Mapping[str, Any]
    _structure_aliases: Mapping[str, str]
    _structure_output_aliases: Mapping[str, tuple[str, ...]]
    _structure_single: bool
    schema: ResultSchemas

    def __init__(
        self,
        outputs: Mapping[str, Any],
        *,
        single: bool = False,
        schema: Mapping[str, Any] | None = None,
        aliases: Mapping[str, tuple[str, ...]] | None = None,
    ) -> None:
        values = dict(outputs)
        output_aliases = dict(aliases or {})
        object.__setattr__(self, "_structure_single", single)
        if single:
            if len(values) != 1:
                raise ValueError("single-output TransformResult requires exactly one output")
        object.__setattr__(self, "_structure_outputs", MappingProxyType(values))
        object.__setattr__(self, "_structure_output_aliases", MappingProxyType(output_aliases))
        object.__setattr__(self, "_structure_aliases", MappingProxyType(self._alias_index(output_aliases)))
        object.__setattr__(self, "schema", ResultSchemas(schema, aliases=output_aliases))

    def __getitem__(self, name: str) -> Any:
        if name in self._structure_outputs:
            return self._structure_outputs[name]
        return self._structure_outputs[self._structure_aliases.get(name, name)]

    def __iter__(self) -> Iterator[str]:
        return iter(self._structure_outputs)

    def __len__(self) -> int:
        return len(self._structure_outputs)

    def __getattr__(self, name: str) -> Any:
        if name in self._structure_outputs:
            return self._structure_outputs[name]
        if name in self._structure_aliases:
            return self._structure_outputs[self._structure_aliases[name]]
        raise AttributeError(name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("TransformResult is read-only")

    def as_dict(self) -> dict[str, Any]:
        return dict(self._structure_outputs)

    def _structure_with_schema(
        self,
        schema: Mapping[str, Any],
        *,
        aliases: Mapping[str, tuple[str, ...]] | None = None,
    ) -> TransformResult:
        output_aliases = aliases or self._structure_output_aliases
        object.__setattr__(self, "_structure_output_aliases", MappingProxyType(dict(output_aliases)))
        object.__setattr__(self, "_structure_aliases", MappingProxyType(self._alias_index(output_aliases)))
        object.__setattr__(self, "schema", ResultSchemas(schema, aliases=output_aliases))
        return self

    def _alias_index(self, aliases: Mapping[str, tuple[str, ...]]) -> dict[str, str]:
        indexed: dict[str, str] = {}
        for name, names in aliases.items():
            for alias in names:
                existing = indexed.get(alias)
                if existing is not None and existing != name:
                    raise ValueError(f"TransformResult alias {alias} points to both {existing} and {name}")
                indexed[alias] = name
        return indexed
