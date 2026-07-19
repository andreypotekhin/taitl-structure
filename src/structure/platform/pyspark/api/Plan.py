from structure.platform.pyspark.capabilities.PySparkCapabilityRules import PySparkCapabilities
from structure.platform.pyspark.commands.LowerPySparkPlan import LowerPySparkPlan


class Plan:

    @staticmethod
    def lower() -> LowerPySparkPlan:
        return LowerPySparkPlan(PySparkCapabilities())
