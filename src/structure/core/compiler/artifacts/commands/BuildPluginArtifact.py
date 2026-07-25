from structure.core.compiler.api.Compiler import Compiler
from structure.core.compiler.artifacts.model.PluginArtifact import PluginArtifact
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.core.plugins.api.Plugin import Plugin
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.plugin.api.v1.model import CompileRequest, PluginCompilation


class BuildPluginArtifact:
    def __init__(self, registry) -> None:
        self._registry = registry
        self._target = Plugin.resolve_target()

    def __call__(
        self,
        transform: type[Transform] | Transform | TransformPipeline,
        *,
        configuration: PluginConfiguration,
        target: str | None = None,
    ) -> PluginArtifact:
        name = self._target(transform, configuration=configuration, target=target)
        plugin = self._registry.select(name, disabled_distributions=configuration.disabled_distributions)
        compilation = (
            Compiler.frontend.compile()(
                transform,
                overrides={"plugin": {"default": name, name: dict(configuration.plugins.get(name, {}))}},
                registry=self._registry,
                materialize_schemas=False,
            )
            if isinstance(transform, type) and transform._structure_outputs
            else plugin.api.compiler.compile(
                CompileRequest(transform=transform, target=name, configuration=configuration.plugins.get(name, {}))
            )
        )
        if not isinstance(compilation, PluginCompilation):
            raise ValueError(f"PLUGIN-E2708: Plugin {name!r} returned an invalid compilation result.")
        return PluginArtifact(
            plugin=name,
            distribution=plugin.descriptor.distribution,
            plugin_version=plugin.descriptor.plugin_version,
            api_version=plugin.api_version,
            configuration=tuple(sorted(configuration.plugins.get(name, {}).items())),
            fingerprint=compilation.fingerprint,
            payload=compilation.lowered,
            analysis=compilation.analysis,
        )
