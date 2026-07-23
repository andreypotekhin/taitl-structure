from structure.plugin.api.v1 import ExplainAPI as ExplainAPIV1
from structure.plugin.api.v1 import ExplainRequest
from structure.plugin.pyspark.api.PySpark import PySpark


class ExplainAPI(ExplainAPIV1):
    def render(self, request: ExplainRequest) -> str:
        return PySpark.render.explain()(request)
