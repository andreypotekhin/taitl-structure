from __future__ import annotations

from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration


class ResolvePluginTarget:
    def __call__(
        self,
        subject: type[Transform] | Transform | TransformPipeline,
        *,
        configuration: PluginConfiguration,
        target: str | None = None,
    ) -> str:
        if isinstance(subject, TransformPipeline):
            return self._pipeline(subject, configuration=configuration, target=target)
        transform = subject if isinstance(subject, type) else type(subject)
        declared = transform._structure_transform_options.get("target")
        if declared is not None and not isinstance(declared, str):
            raise TypeError("Transform target must be a string.")
        if target is not None and declared is not None and target != declared:
            raise ValueError(
                f"PLUGIN-E2703: Explicit plugin {target!r} conflicts with transform plugin {declared!r}."
            )
        resolved = declared or target or configuration.default
        if resolved is None:
            raise ValueError("PLUGIN-E2702: No plugin target was resolved for the transform.")
        return resolved

    def _pipeline(
        self,
        pipeline: TransformPipeline,
        *,
        configuration: PluginConfiguration,
        target: str | None,
    ) -> str:
        targets = {self(stage.invocation, configuration=configuration, target=target) for stage in pipeline.stages}
        if len(targets) != 1:
            raise ValueError("PLUGIN-E2711: Composed transforms resolve to different plugins.")
        return targets.pop()
