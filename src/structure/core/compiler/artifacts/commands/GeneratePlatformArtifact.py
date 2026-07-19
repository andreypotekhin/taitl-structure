from structure.core.compiler.artifacts.model.PlatformArtifact import PlatformArtifact
from structure.core.platforms.model.PlatformConfiguration import PlatformConfiguration
from structure.platform.api.v1.model import GenerationRequest


class GeneratePlatformArtifact:
    def __init__(self, registry) -> None:
        self._registry = registry

    def __call__(self, artifact: PlatformArtifact, *, configuration: PlatformConfiguration) -> dict[str, str]:
        platform = self._registry.select(artifact.platform, disabled_distributions=configuration.disabled_distributions)
        descriptor = platform.descriptor
        if (descriptor.distribution, descriptor.plugin_version, platform.api_version) != (
            artifact.distribution,
            artifact.plugin_version,
            artifact.api_version,
        ):
            raise ValueError("PLATFORM-E2710: Artifact identity is incompatible with the selected platform.")
        if platform.api.generator is None:
            raise ValueError(f"PLATFORM-E2709: Platform {artifact.platform!r} does not provide generation.")
        return dict(platform.api.generator.generate(GenerationRequest(payload=artifact.payload)))
