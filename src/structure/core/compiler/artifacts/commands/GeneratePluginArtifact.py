from structure.core.compiler.artifacts.model.PluginArtifact import PluginArtifact
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.plugin.api.v1.model import GenerationRequest


class GeneratePluginArtifact:
    def __init__(self, registry) -> None:
        self._registry = registry

    def __call__(self, artifact: PluginArtifact, *, configuration: PluginConfiguration) -> dict[str, str]:
        plugin = self._registry.select(artifact.plugin, disabled_distributions=configuration.disabled_distributions)
        descriptor = plugin.descriptor
        if (descriptor.distribution, descriptor.plugin_version, plugin.api_version) != (
            artifact.distribution,
            artifact.plugin_version,
            artifact.api_version,
        ):
            raise ValueError("PLUGIN-E2710: Artifact identity is incompatible with the selected plugin.")
        if plugin.api.generator is None:
            raise ValueError(f"PLUGIN-E2709: Plugin {artifact.plugin!r} does not provide generation.")
        return dict(plugin.api.generator.generate(GenerationRequest(payload=artifact.payload)))
