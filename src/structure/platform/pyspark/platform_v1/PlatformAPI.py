from structure.platform.api.v1 import PlatformAPI as V1PlatformAPI
from structure.platform.pyspark.platform_v1.Analysis import Analysis
from structure.platform.pyspark.platform_v1.Capabilities import Capabilities
from structure.platform.pyspark.platform_v1.Compiler import Compiler
from structure.platform.pyspark.platform_v1.Execution import Execution
from structure.platform.pyspark.platform_v1.Explain import Explain
from structure.platform.pyspark.platform_v1.Generation import Generation
from structure.platform.pyspark.platform_v1.Schema import Schema


class PlatformAPI:
    def create(self) -> V1PlatformAPI:
        return V1PlatformAPI(
            schema=Schema(),
            compiler=Compiler(),
            capabilities=Capabilities(),
            executor=Execution(),
            generator=Generation(),
            explainer=Explain(),
            analysis=Analysis(),
        )
