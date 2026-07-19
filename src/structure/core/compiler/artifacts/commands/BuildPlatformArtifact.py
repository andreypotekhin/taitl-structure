from structure.core.compiler.artifacts.model.PlatformArtifact import PlatformArtifact
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.core.platforms.api.Platform import Platform
from structure.core.platforms.model.PlatformConfiguration import PlatformConfiguration
from structure.platform.api.v1.CompileRequest import CompileRequest
from structure.platform.api.v1.PlatformCompilation import PlatformCompilation


class BuildPlatformArtifact:
    def __init__(self, registry) -> None:
        self._registry = registry
        self._target = Platform.resolve_target()

    def __call__(
        self,
        transform: type[Transform] | Transform | TransformPipeline,
        *,
        configuration: PlatformConfiguration,
        target: str | None = None,
    ) -> PlatformArtifact:
        name = self._target(transform, configuration=configuration, target=target)
        platform = self._registry.select(name, disabled_distributions=configuration.disabled_distributions)
        compilation = platform.api.compiler.compile(
            CompileRequest(transform=transform, target=name, configuration=configuration.plugins.get(name, {}))
        )
        if not isinstance(compilation, PlatformCompilation):
            raise ValueError(f"PLATFORM-E2708: Platform {name!r} returned an invalid compilation result.")
        return PlatformArtifact(
            platform=name,
            distribution=platform.descriptor.distribution,
            plugin_version=platform.descriptor.plugin_version,
            api_version=platform.api_version,
            configuration=tuple(sorted(configuration.plugins.get(name, {}).items())),
            fingerprint=compilation.fingerprint,
            payload=compilation.lowered,
            analysis=compilation.analysis,
        )
