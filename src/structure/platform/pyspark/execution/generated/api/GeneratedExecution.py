from structure.platform.pyspark.execution.generated.commands.RunGeneratedPySparkTransform import (
    RunGeneratedPySparkTransform,
)


class GeneratedExecution:

    @staticmethod
    def pyspark() -> RunGeneratedPySparkTransform:
        return RunGeneratedPySparkTransform()
