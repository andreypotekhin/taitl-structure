from structure.core.compiler.api.Compiler import Compiler
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.platforms.api.Platform import Platform
from structure.core.platforms.model.PlatformConfiguration import PlatformConfiguration
from structure.platform.api.v1.model import ExplainRequest


class RenderExplainReport:
    def __call__(self, transform: type[Transform], *, config: StructureConfig | None = None) -> str:
        resolved = config or StructureConfig.resolve()
        compilation = Compiler.frontend.compile()(transform, config=resolved, materialize_schemas=False)
        configuration = PlatformConfiguration.resolve({"platform": {"default": resolved.target_backend}})
        target = Platform.resolve_target()(transform, configuration=configuration)
        platform = Platform.registry().select(target)
        if platform.api.explainer is None:
            raise ValueError(f"PLATFORM-E2709: Platform {target!r} does not provide explain rendering.")
        return platform.api.explainer.render(
            ExplainRequest(transform=transform, payload=compilation.lowered, analysis=compilation.analysis)
        )


render_explain_report = RenderExplainReport()
