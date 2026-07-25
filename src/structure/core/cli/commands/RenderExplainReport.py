from structure.core.compiler.api.Compiler import Compiler
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.plugins.api.Plugin import Plugin
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.plugin.api.v1.model import ExplainRequest


class RenderExplainReport:
    def __call__(self, transform: type[Transform], *, config: StructureConfig | None = None) -> str:
        resolved = config or StructureConfig.resolve()
        compilation = Compiler.frontend.compile()(transform, config=resolved, materialize_schemas=False)
        configuration = PluginConfiguration.resolve({"plugin": {"default": resolved.target}})
        target = Plugin.resolve_target()(transform, configuration=configuration)
        plugin = Plugin.registry().select(target)
        if plugin.api.explainer is None:
            raise ValueError(f"PLUGIN-E2709: Plugin {target!r} does not provide explain rendering.")
        return plugin.api.explainer.render(
            ExplainRequest(transform=transform, payload=compilation.lowered, analysis=compilation.analysis)
        )


render_explain_report = RenderExplainReport()
