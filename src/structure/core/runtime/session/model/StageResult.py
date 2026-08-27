from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Any

from structure.core.runtime.schemas.model.TransformSchemas import ResultSchemas


class StageResult(Mapping[str, Any]):
    """A read-only namespace containing one composed stage's public outputs."""

    def __init__(
        self,
        outputs: Mapping[str, Any],
        *,
        schema: Mapping[str, Any] | None = None,
        aliases: Mapping[str, tuple[str, ...]] | None = None,
        stages: Mapping[str, "StageResult"] | None = None,
    ) -> None:
        output_values = dict(outputs)
        output_aliases = dict(aliases or {})
        object.__setattr__(self, "_structure_outputs", MappingProxyType(output_values))
        object.__setattr__(self, "_structure_output_aliases", MappingProxyType(output_aliases))
        object.__setattr__(self, "_structure_aliases", MappingProxyType(self._alias_index(output_aliases)))
        object.__setattr__(self, "_structure_stages", MappingProxyType(dict(stages or {})))
        object.__setattr__(self, "schema", ResultSchemas(schema, aliases=output_aliases))

    @property
    def stages(self) -> Mapping[str, "StageResult"]:
        return self._structure_stages

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
        if name in self._structure_stages:
            return self._structure_stages[name]
        raise AttributeError(name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("StageResult is read-only")

    def as_dict(self) -> dict[str, Any]:
        return dict(self._structure_outputs)

    @staticmethod
    def _alias_index(aliases: Mapping[str, tuple[str, ...]]) -> dict[str, str]:
        indexed: dict[str, str] = {}
        for name, names in aliases.items():
            for alias in names:
                existing = indexed.get(alias)
                if existing is not None and existing != name:
                    raise ValueError(f"StageResult alias {alias} points to both {existing} and {name}")
                indexed[alias] = name
        return indexed


def build_stage_results(
    records: list[tuple[tuple[str, ...], Any, Any, tuple[str, ...]]],
) -> Mapping[str, StageResult]:
    """Build a recursive stage namespace from flattened output records."""

    root: dict[str, dict[str, Any]] = {}
    for path, value, schema, aliases in records:
        if len(path) < 2:
            raise ValueError("Stage output paths must contain a stage and output name")
        nodes = root
        for stage_name in path[:-1]:
            node = nodes.setdefault(stage_name, {"outputs": {}, "schemas": {}, "aliases": {}, "stages": {}})
            nodes = node["stages"]
        stage_name = path[-2]
        node = root
        for name in path[:-2]:
            node = node[name]["stages"]
        stage = node.setdefault(stage_name, {"outputs": {}, "schemas": {}, "aliases": {}, "stages": {}})
        output_name = path[-1]
        stage["outputs"][output_name] = value
        stage["schemas"][output_name] = schema
        if aliases:
            stage["aliases"][output_name] = aliases

    def freeze(nodes: Mapping[str, dict[str, Any]]) -> Mapping[str, StageResult]:
        return MappingProxyType(
            {
                name: StageResult(
                    node["outputs"],
                    schema=node["schemas"],
                    aliases=node["aliases"],
                    stages=freeze(node["stages"]),
                )
                for name, node in nodes.items()
            }
        )

    return freeze(root)
