from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Any

from structure.core.runtime.schemas.model.TransformSchemas import ResultSchemas
from structure.core.runtime.session.model.ObservationResult import ObservationResult
from structure.core.runtime.session.model.SinkResult import SinkResult
from structure.core.runtime.session.model.StageResult import StageResult, build_stage_results


class TransformResult(Mapping[str, Any]):
    _structure_outputs: Mapping[str, Any]
    _structure_aliases: Mapping[str, str]
    _structure_output_aliases: Mapping[str, tuple[str, ...]]
    _structure_sinks: Mapping[str, SinkResult]
    _structure_observations: Mapping[str, ObservationResult]
    _structure_single: bool
    state_budget: Mapping[str, object]
    schema: ResultSchemas

    def __init__(
        self,
        outputs: Mapping[str, Any],
        *,
        single: bool = False,
        schema: Mapping[str, Any] | None = None,
        aliases: Mapping[str, tuple[str, ...]] | None = None,
        stage_records: list[tuple[tuple[str, ...], Any, Any, tuple[str, ...]]] | None = None,
        stage_observations: list[tuple[tuple[str, ...], str, ObservationResult]] | None = None,
        unused_stage_observations: list[tuple[tuple[str, ...], str]] | None = None,
        stages: Mapping[str, StageResult] | None = None,
        sinks: Mapping[str, SinkResult] | None = None,
        observations: Mapping[str, ObservationResult] | None = None,
        unused_observations: tuple[str, ...] = (),
        stage_outputs_enabled: bool = True,
        stage_names: tuple[str, ...] = (),
        state_budget: Mapping[str, object] | None = None,
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
        sink_values = dict(sinks or {})
        collisions = set(sink_values) & (set(values) | set(output_aliases) | set(self._alias_index(output_aliases)))
        if collisions:
            raise ValueError(f"TransformResult sink names collide with outputs or aliases: {', '.join(sorted(collisions))}")
        reserved = set(sink_values) & (set(dir(type(self))) | {"schema"})
        if reserved:
            raise ValueError(f"TransformResult sink names are reserved: {', '.join(sorted(reserved))}")
        object.__setattr__(self, "_structure_sinks", MappingProxyType(sink_values))
        observation_values = dict(observations or {})
        all_public_names = set(values) | set(output_aliases) | set(self._alias_index(output_aliases)) | set(sink_values)
        collisions = set(observation_values) & all_public_names
        if collisions:
            raise ValueError(f"TransformResult observation names collide with outputs or sinks: {', '.join(sorted(collisions))}")
        reserved = set(observation_values) & (set(dir(type(self))) | {"schema"})
        if reserved:
            raise ValueError(f"TransformResult observation names are reserved: {', '.join(sorted(reserved))}")
        object.__setattr__(self, "_structure_observations", MappingProxyType(observation_values))
        object.__setattr__(self, "_structure_unused_observations", frozenset(unused_observations))
        stage_values = (
            build_stage_results(stage_records or [], stage_observations, unused_stage_observations)
            if stage_records is not None or stage_observations is not None or unused_stage_observations is not None
            else dict(stages or {})
        )
        object.__setattr__(self, "_structure_stages", MappingProxyType(dict(stage_values)))
        object.__setattr__(self, "_structure_stage_outputs_enabled", stage_outputs_enabled)
        object.__setattr__(self, "_structure_stage_names", frozenset(stage_names))
        object.__setattr__(self, "state_budget", MappingProxyType(dict(state_budget or {})))
        object.__setattr__(self, "schema", ResultSchemas(schema, aliases=output_aliases))

    @property
    def stages(self) -> Mapping[str, StageResult]:
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
        if name in self._structure_sinks:
            return self._structure_sinks[name]
        if name in self._structure_observations:
            return self._structure_observations[name]
        if name in self._structure_unused_observations:
            raise AttributeError(
                f"Observation {name!r} was declared but is unused in this transform specialization."
            )
        if name in self._structure_aliases:
            return self._structure_outputs[self._structure_aliases[name]]
        if name in self._structure_stages:
            return self._structure_stages[name]
        if name in self._structure_stage_names and not self._structure_stage_outputs_enabled:
            raise AttributeError(
                f"Stage output access is disabled for {name!r}; set allow_stage_outputs=True to enable it"
            )
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

    def _structure_with_sinks(self, sinks: Mapping[str, SinkResult]) -> TransformResult:
        """Attach named sink handoffs without changing output mapping behavior."""
        sink_values = dict(sinks)
        collisions = set(sink_values) & (
            set(self._structure_outputs) | set(self._structure_output_aliases) | set(self._structure_aliases)
        )
        if collisions:
            raise ValueError(f"TransformResult sink names collide with outputs or aliases: {', '.join(sorted(collisions))}")
        reserved = set(sink_values) & (set(dir(type(self))) | {"schema"})
        if reserved:
            raise ValueError(f"TransformResult sink names are reserved: {', '.join(sorted(reserved))}")
        object.__setattr__(self, "_structure_sinks", MappingProxyType(sink_values))
        return self

    def _structure_with_state_budget(self, state_budget: Mapping[str, object]) -> TransformResult:
        object.__setattr__(self, "state_budget", MappingProxyType(dict(state_budget)))
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
