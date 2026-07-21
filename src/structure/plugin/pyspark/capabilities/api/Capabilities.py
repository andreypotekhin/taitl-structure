from structure.plugin.pyspark.capabilities.commands.CheckSparkConnectCompatibility import CheckSparkConnectCompatibility
from structure.plugin.pyspark.capabilities.commands.ResolvePySparkCapabilities import ResolvePySparkCapabilities


class Capabilities:

    def resolve(self) -> ResolvePySparkCapabilities:
        return ResolvePySparkCapabilities()

    def spark_connect(self) -> CheckSparkConnectCompatibility:
        return CheckSparkConnectCompatibility()
