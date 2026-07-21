from structure.plugin.api.v1 import PluginAPIV1
from structure.plugin.pyspark.api.Analysis import Analysis
from structure.plugin.pyspark.api.Authoring import Authoring
from structure.plugin.pyspark.api.Capabilities import Capabilities
from structure.plugin.pyspark.api.Compiler import Compiler
from structure.plugin.pyspark.api.Execution import Execution
from structure.plugin.pyspark.api.Explain import Explain
from structure.plugin.pyspark.api.Generation import Generation
from structure.plugin.pyspark.api.Schema import Schema


class PluginAPI:
    def create(self) -> PluginAPIV1:
        return PluginAPIV1(
            schema=Schema(),
            compiler=Compiler(),
            capabilities=Capabilities(),
            authoring=Authoring(),
            executor=Execution(),
            generator=Generation(),
            explainer=Explain(),
            analysis=Analysis(),
        )
