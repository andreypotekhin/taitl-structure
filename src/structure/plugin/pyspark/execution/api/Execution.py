from structure.plugin.pyspark.execution.commands.RunGeneratedPySparkTransform import RunGeneratedPySparkTransform
from structure.plugin.pyspark.execution.commands.RunOnlinePySparkTransform import RunOnlinePySparkTransform
from structure.plugin.pyspark.execution.logic.expressions.EvaluatePySparkExpression import EvaluatePySparkExpression
from structure.plugin.pyspark.execution.logic.ValidatePySparkFrame import ValidatePySparkFrame


class Execution:

    def expression(self) -> EvaluatePySparkExpression:
        return EvaluatePySparkExpression()

    def validator(self) -> ValidatePySparkFrame:
        return ValidatePySparkFrame()

    def generated(self) -> RunGeneratedPySparkTransform:
        return RunGeneratedPySparkTransform()

    def online(self) -> RunOnlinePySparkTransform:
        return RunOnlinePySparkTransform()
