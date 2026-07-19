from structure.core.compiler.artifacts.model.PlatformArtifact import PlatformArtifact
from structure.core.platforms.model.PlatformConfiguration import PlatformConfiguration
from structure.platform.api.v1.model import ExecutionRequest


class ExecutePlatformArtifact:
    def __init__(self, registry) -> None:
        self._registry = registry

    def __call__(self, artifact: PlatformArtifact, *, configuration: PlatformConfiguration, runtime: object) -> object:
        platform = self._registry.select(artifact.platform, disabled_distributions=configuration.disabled_distributions)
        descriptor = platform.descriptor
        if (
            descriptor.distribution != artifact.distribution
            or descriptor.plugin_version != artifact.plugin_version
            or platform.api_version != artifact.api_version
        ):
            raise ValueError("PLATFORM-E2710: Artifact identity is incompatible with the selected platform.")
        if platform.api.executor is None:
            raise ValueError(f"PLATFORM-E2709: Platform {artifact.platform!r} does not provide execution.")
        return platform.api.executor.execute(ExecutionRequest(payload=artifact.payload, runtime=runtime))
