from structure.core.compiler.artifacts.model.PluginArtifact import PluginArtifact
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.plugin.api.v1.model import ExecutionRequest


class ExecutePluginArtifact:
    def __init__(self, registry) -> None:
        self._registry = registry

    def __call__(self, artifact: PluginArtifact, *, configuration: PluginConfiguration, runtime: object) -> object:
        plugin = self._registry.select(artifact.plugin, disabled_distributions=configuration.disabled_distributions)
        descriptor = plugin.descriptor
        if (
            descriptor.distribution != artifact.distribution
            or descriptor.plugin_version != artifact.plugin_version
            or plugin.api_version != artifact.api_version
        ):
            raise ValueError("PLUGIN-E2710: Artifact identity is incompatible with the selected plugin.")
        if plugin.api.executor is None:
            raise ValueError(f"PLUGIN-E2709: Plugin {artifact.plugin!r} does not provide execution.")
        return plugin.api.executor.execute(ExecutionRequest(payload=artifact.payload, runtime=runtime))
