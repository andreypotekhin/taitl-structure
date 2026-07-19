from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.platforms.api.Platform import Platform
from structure.platform.api.v1.ExplainRequest import ExplainRequest


class RenderExplainReport:
    def __call__(self, transform: type[Transform]) -> str:
        target = transform._structure_transform_options.get("target", "pyspark")
        if not isinstance(target, str):
            raise TypeError("Transform target must be a string.")
        platform = Platform.registry().select(target)
        if platform.api.explainer is None:
            raise ValueError(f"PLATFORM-E2709: Platform {target!r} does not provide explain rendering.")
        return platform.api.explainer.render(ExplainRequest(transform=transform))


render_explain_report = RenderExplainReport()
