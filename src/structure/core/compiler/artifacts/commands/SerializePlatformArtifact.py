from dataclasses import replace

from structure.core.compiler.artifacts.model.PlatformArtifact import PlatformArtifact
from structure.core.platform.PlatformConfiguration import PlatformConfiguration
from structure.core.platform.PlatformRegistry import PlatformRegistry


class SerializePlatformArtifact:
    def __init__(self, registry: PlatformRegistry) -> None:
        self._registry = registry

    def encode(self, artifact: PlatformArtifact, *, configuration: PlatformConfiguration) -> bytes:
        return self._serializer(artifact, configuration).encode(artifact.payload)

    def decode(self, artifact: PlatformArtifact, payload: bytes, *, configuration: PlatformConfiguration) -> PlatformArtifact:
        return replace(artifact, payload=self._serializer(artifact, configuration).decode(payload))

    def _serializer(self, artifact: PlatformArtifact, configuration: PlatformConfiguration):
        selected = self._registry.select(artifact.platform, disabled_distributions=configuration.disabled_distributions)
        descriptor = selected.descriptor
        if (descriptor.distribution, descriptor.plugin_version, selected.api_version) != (
            artifact.distribution,
            artifact.plugin_version,
            artifact.api_version,
        ):
            raise ValueError("PLATFORM-E2710: Artifact identity is incompatible with the selected platform.")
        if selected.api.serializer is None:
            raise ValueError(f"PLATFORM-E2709: Platform {artifact.platform!r} does not provide serialization.")
        return selected.api.serializer
