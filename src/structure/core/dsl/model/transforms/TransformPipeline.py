from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from structure.core.dsl.model.transforms.Transform import Transform


@dataclass(frozen=True)
class TransformPipelineStage:
    invocation: Transform

    @property
    def transform_class(self) -> type[Transform]:
        return type(self.invocation)


class TransformPipeline:
    _structure_pipeline = True

    def __init__(self, stages: Iterable[Transform]) -> None:
        flattened = tuple(self._flatten(stages))
        if not flattened:
            raise TypeError("Transform.to(...) requires at least one transform invocation")
        self.stages = tuple(TransformPipelineStage(stage) for stage in flattened)
        self._structure_bound_inputs = self._bound_inputs()

    def to(self, *stages: Transform) -> TransformPipeline:
        return TransformPipeline((*self.invocations, *stages))

    def run(self, session):
        return session.run(self)

    def compile(
        self,
        options=None,
        *,
        project_root=None,
        config=None,
        schema_types=None,
        force: bool = False,
        platform_configuration=None,
        platform_registry=None,
        target: str | None = None,
        **settings: object,
    ):
        from structure.core.compiler.artifacts.commands import BuildCompiledTransform, BuildPlatformArtifact
        from structure.core.compiler.artifacts.model import CompilerOptions

        if platform_configuration is not None or platform_registry is not None:
            if platform_configuration is None or platform_registry is None:
                raise ValueError("platform_configuration and platform_registry must be supplied together.")
            return BuildPlatformArtifact(platform_registry)(self, configuration=platform_configuration, target=target)
        resolved = CompilerOptions.resolve(
            options,
            project_root=project_root,
            config=config,
            schema_types=schema_types,
            overrides=settings,
        )
        return BuildCompiledTransform()(self, options=resolved, schema_types=schema_types)

    @property
    def invocations(self) -> tuple[Transform, ...]:
        return tuple(stage.invocation for stage in self.stages)

    def _flatten(self, stages: Iterable[Transform]) -> Iterable[Transform]:
        from structure.core.dsl.model.transforms.Transform import Transform

        for stage in stages:
            if isinstance(stage, TransformPipeline):
                yield from stage.invocations
                continue
            if not isinstance(stage, Transform):
                raise TypeError("Transform.to(...) accepts transform invocations, not transform classes")
            yield stage

    def _bound_inputs(self) -> dict[str, object]:
        values: dict[str, object] = {}
        for stage in self.stages:
            for name, value in stage.invocation._structure_bound_inputs.items():
                existing = values.get(name)
                if existing is not None and existing is not value:
                    raise TypeError(
                        f"Composed transform input {name} is bound more than once. "
                        "Use distinct input names or bind it once."
                    )
                values[name] = value
        return values
