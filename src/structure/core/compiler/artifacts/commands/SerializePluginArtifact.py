from dataclasses import replace

from structure.core.compiler.artifacts.model.PluginArtifact import PluginArtifact
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration


class SerializePluginArtifact:
    def __init__(self, registry) -> None:
        self._registry = registry

    def encode(self, artifact: PluginArtifact, *, configuration: PluginConfiguration) -> bytes:
        return self._serializer(artifact, configuration).encode(artifact.payload)

    def decode(self, artifact: PluginArtifact, payload: bytes, *, configuration: PluginConfiguration) -> PluginArtifact:
        return replace(artifact, payload=self._serializer(artifact, configuration).decode(payload))

    def _serializer(self, artifact: PluginArtifact, configuration: PluginConfiguration):
        plugin = self._registry.select(artifact.plugin, disabled_distributions=configuration.disabled_distributions)
        descriptor = plugin.descriptor
        if (descriptor.distribution, descriptor.plugin_version, plugin.api_version) != (
            artifact.distribution,
            artifact.plugin_version,
            artifact.api_version,
        ):
            raise ValueError("PLUGIN-E2710: Artifact identity is incompatible with the selected plugin.")
        if plugin.api.serializer is None:
            raise ValueError(f"PLUGIN-E2709: Plugin {artifact.plugin!r} does not provide serialization.")
        return plugin.api.serializer
