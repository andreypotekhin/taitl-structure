from structure.plugin.pyspark.execution.commands.RunGeneratedPySparkTransform import RunGeneratedPySparkTransform
from structure.plugin.pyspark.execution.commands.RunOnlinePySparkTransform import RunOnlinePySparkTransform


class Execution:

    def generated(self) -> RunGeneratedPySparkTransform:
        return RunGeneratedPySparkTransform()

    def online(self) -> RunOnlinePySparkTransform:
        return RunOnlinePySparkTransform()
