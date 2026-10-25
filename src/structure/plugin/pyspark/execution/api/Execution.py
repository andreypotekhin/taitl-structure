from structure.plugin.pyspark.execution.commands.RunGeneratedPySparkTransform import RunGeneratedPySparkTransform
from structure.plugin.pyspark.execution.commands.RunOnlinePySparkTransform import RunOnlinePySparkTransform
from structure.plugin.pyspark.execution.logic.expressions.EvaluatePySparkExpression import EvaluatePySparkExpression
from structure.plugin.pyspark.execution.logic.ValidatePySparkFrame import ValidatePySparkFrame
from structure.plugin.pyspark.execution.logic.ValidatePySparkRuntime import validate_pyspark_runtime


class Execution:

    def expression(self) -> EvaluatePySparkExpression:
        return EvaluatePySparkExpression()

    def validator(self) -> ValidatePySparkFrame:
        return ValidatePySparkFrame()

    def generated(self) -> RunGeneratedPySparkTransform:
        return RunGeneratedPySparkTransform()

    def online(self) -> RunOnlinePySparkTransform:
        return RunOnlinePySparkTransform()

    def validate_runtime(self, session, *, transform: str = "", execution_mode: str = "online") -> None:
        validate_pyspark_runtime(session, transform=transform, execution_mode=execution_mode)
