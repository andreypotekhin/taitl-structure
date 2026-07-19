from dataclasses import replace

from structure.core.compiler.artifacts.model.PlatformArtifact import PlatformArtifact
from structure.core.platforms.model.PlatformConfiguration import PlatformConfiguration


class SerializePlatformArtifact:
    def __init__(self, registry) -> None:
        self._registry = registry

    def encode(self, artifact: PlatformArtifact, *, configuration: PlatformConfiguration) -> bytes:
        return self._serializer(artifact, configuration).encode(artifact.payload)

    def decode(self, artifact: PlatformArtifact, payload: bytes, *, configuration: PlatformConfiguration) -> PlatformArtifact:
        return replace(artifact, payload=self._serializer(artifact, configuration).decode(payload))

    def _serializer(self, artifact: PlatformArtifact, configuration: PlatformConfiguration):
        platform = self._registry.select(artifact.platform, disabled_distributions=configuration.disabled_distributions)
        descriptor = platform.descriptor
        if (descriptor.distribution, descriptor.plugin_version, platform.api_version) != (
            artifact.distribution,
            artifact.plugin_version,
            artifact.api_version,
        ):
            raise ValueError("PLATFORM-E2710: Artifact identity is incompatible with the selected platform.")
        if platform.api.serializer is None:
            raise ValueError(f"PLATFORM-E2709: Platform {artifact.platform!r} does not provide serialization.")
        return platform.api.serializer
