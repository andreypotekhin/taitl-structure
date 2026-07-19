from __future__ import annotations

from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.core.platforms.model.PlatformConfiguration import PlatformConfiguration


class ResolvePlatformTarget:
    def __call__(
        self,
        subject: type[Transform] | Transform | TransformPipeline,
        *,
        configuration: PlatformConfiguration,
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
                f"PLATFORM-E2703: Explicit platform {target!r} conflicts with transform platform {declared!r}."
            )
        resolved = declared or target or configuration.default
        if resolved is None:
            raise ValueError("PLATFORM-E2702: No platform target was resolved for the transform.")
        return resolved

    def _pipeline(
        self,
        pipeline: TransformPipeline,
        *,
        configuration: PlatformConfiguration,
        target: str | None,
    ) -> str:
        targets = {self(stage.invocation, configuration=configuration, target=target) for stage in pipeline.stages}
        if len(targets) != 1:
            raise ValueError("PLATFORM-E2711: Composed transforms resolve to different platforms.")
        return targets.pop()
