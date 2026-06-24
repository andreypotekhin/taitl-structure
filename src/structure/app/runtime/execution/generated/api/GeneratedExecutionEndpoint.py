from structure.app.runtime.execution.generated.logic.actions.RunGeneratedPySparkTransform import (
    RunGeneratedPySparkTransform,
)


class GeneratedExecutionEndpoint:

    def pyspark(self) -> RunGeneratedPySparkTransform:
        return RunGeneratedPySparkTransform()
