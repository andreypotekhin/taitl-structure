from structure.plugin.api.v1 import ExplainAPI, ExplainRequest
from structure.plugin.pyspark.api.PySpark import PySpark


class Explain(ExplainAPI):
    def render(self, request: ExplainRequest) -> str:
        return PySpark.render.explain()(request)
