from structure.core.compiler.artifacts.model.PlatformArtifact import PlatformArtifact
from structure.core.platform.PlatformConfiguration import PlatformConfiguration
from structure.core.platform.PlatformRegistry import PlatformRegistry


class ExecutePlatformArtifact:
    def __init__(self, registry: PlatformRegistry) -> None:
        self._registry = registry

    def __call__(self, artifact: PlatformArtifact, *, configuration: PlatformConfiguration, runtime: object) -> object:
        selected = self._registry.select(artifact.platform, disabled_distributions=configuration.disabled_distributions)
        descriptor = selected.descriptor
        if (
            descriptor.distribution != artifact.distribution
            or descriptor.plugin_version != artifact.plugin_version
            or selected.api_version != artifact.api_version
        ):
            raise ValueError("PLATFORM-E2710: Artifact identity is incompatible with the selected platform.")
        if selected.api.executor is None:
            raise ValueError(f"PLATFORM-E2709: Platform {artifact.platform!r} does not provide execution.")
        return selected.api.executor.execute(artifact.payload, runtime)
