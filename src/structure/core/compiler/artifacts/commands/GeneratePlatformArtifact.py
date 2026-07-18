from structure.core.compiler.artifacts.model.PlatformArtifact import PlatformArtifact
from structure.core.platform.PlatformConfiguration import PlatformConfiguration
from structure.core.platform.PlatformRegistry import PlatformRegistry


class GeneratePlatformArtifact:
    def __init__(self, registry: PlatformRegistry) -> None:
        self._registry = registry

    def __call__(self, artifact: PlatformArtifact, *, configuration: PlatformConfiguration) -> dict[str, str]:
        selected = self._registry.select(artifact.platform, disabled_distributions=configuration.disabled_distributions)
        descriptor = selected.descriptor
        if (descriptor.distribution, descriptor.plugin_version, selected.api_version) != (
            artifact.distribution,
            artifact.plugin_version,
            artifact.api_version,
        ):
            raise ValueError("PLATFORM-E2710: Artifact identity is incompatible with the selected platform.")
        if selected.api.generator is None:
            raise ValueError(f"PLATFORM-E2709: Platform {artifact.platform!r} does not provide generation.")
        return dict(selected.api.generator.generate(artifact.payload))
