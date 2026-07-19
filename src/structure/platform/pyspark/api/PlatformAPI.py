from structure.platform.api.v1 import PlatformAPIV1
from structure.platform.pyspark.api.Analysis import Analysis
from structure.platform.pyspark.api.Authoring import Authoring
from structure.platform.pyspark.api.Capabilities import Capabilities
from structure.platform.pyspark.api.Compiler import Compiler
from structure.platform.pyspark.api.Execution import Execution
from structure.platform.pyspark.api.Explain import Explain
from structure.platform.pyspark.api.Generation import Generation
from structure.platform.pyspark.api.Schema import Schema


class PlatformAPI:
    def create(self) -> PlatformAPIV1:
        return PlatformAPIV1(
            schema=Schema(),
            compiler=Compiler(),
            capabilities=Capabilities(),
            authoring=Authoring(),
            executor=Execution(),
            generator=Generation(),
            explainer=Explain(),
            analysis=Analysis(),
        )
