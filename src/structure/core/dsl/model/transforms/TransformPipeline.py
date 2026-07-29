"""Linear transform pipelines created with ``Transform.to(...)``."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from structure.core.dsl.model.transforms.Transform import Transform


@dataclass(frozen=True)
class TransformPipelineStage:
    """One transform invocation in a composed pipeline."""

    invocation: Transform

    @property
    def transform_class(self) -> type[Transform]:
        """Return the class that declared this stage."""
        return type(self.invocation)


class TransformPipeline:
    """A runnable and compilable sequence of transform invocations.

    Args:
        stages: Transform instances or existing pipelines. Nested pipelines are
            flattened so chained ``to(...)`` calls remain simple.

    Raises:
        TypeError: No stages are supplied, a stage is not a transform
            invocation, or the same composed input is bound to different values.

    Example:
        pipeline = LoadOrders(raw_orders).to(EnrichOrders(customers), Publish())
        generated = pipeline.compile(target="pyspark")
    """

    _structure_pipeline = True

    def __init__(self, stages: Iterable[Transform]) -> None:
        flattened = tuple(self._flatten(stages))
        if not flattened:
            raise TypeError("Transform.to(...) requires at least one transform invocation")
        self.stages = tuple(TransformPipelineStage(stage) for stage in flattened)
        self._structure_bound_inputs = self._bound_inputs()

    def to(self, *stages: Transform) -> TransformPipeline:
        """Append transform invocations and return a new pipeline."""
        return TransformPipeline((*self.invocations, *stages))

    def run(self, session):
        """Run this pipeline through a Structure session."""
        return session.run(self)

    def compile(
        self,
        options=None,
        *,
        project_root=None,
        config=None,
        schema_types=None,
        force: bool = False,
        plugin_configuration=None,
        plugin_registry=None,
        target: str | None = None,
        **settings: object,
    ):
        """Compile this pipeline with the configured target plugin.

        Args:
            options: Optional compiler options or project source object.
            project_root: Optional project root used for configuration lookup.
            config: Explicit Structure configuration object.
            schema_types: Optional schema type registry override.
            force: Rebuild even when cached artifacts may exist.
            plugin_configuration: Advanced plugin configuration override.
            plugin_registry: Advanced plugin registry override.
            target: Optional target name, such as ``"pyspark"``.
            **settings: Compiler option overrides.

        Returns:
            A compiler artifact for the pipeline.
        """
        from structure.core.compiler.api.Compiler import Compiler
        from structure.core.compiler.artifacts.model import CompilerOptions

        if plugin_configuration is not None or plugin_registry is not None:
            if plugin_configuration is None or plugin_registry is None:
                raise ValueError("plugin_configuration and plugin_registry must be supplied together.")
            return Compiler.artifacts.plugin(plugin_registry)(self, configuration=plugin_configuration, target=target)
        resolved = CompilerOptions.resolve(
            options,
            project_root=project_root,
            config=config,
            schema_types=schema_types,
            overrides=settings,
        )
        return Compiler.artifacts.build()(self, options=resolved, schema_types=schema_types)

    @property
    def invocations(self) -> tuple[Transform, ...]:
        """Return transform invocations in execution order."""
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
