from structure.platform.api.v1 import ExplainAPI, ExplainRequest
from structure.platform.pyspark.commands.RenderPySparkExplainReport import RenderPySparkExplainReport


class Explain(ExplainAPI):
    def render(self, request: ExplainRequest) -> str:
        return RenderPySparkExplainReport()(request)
