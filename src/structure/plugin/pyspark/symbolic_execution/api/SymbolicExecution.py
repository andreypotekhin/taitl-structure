from structure.plugin.pyspark.symbolic_execution.commands.CapturePySparkStep import CapturePySparkStep
from structure.plugin.pyspark.symbolic_execution.commands.OpenPySparkStep import OpenPySparkStep
from structure.plugin.pyspark.symbolic_execution.commands.RewritePySparkStepBody import RewritePySparkStepBody
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkComparisons import ValidatePySparkComparisons
from structure.plugin.pyspark.symbolic_execution.commands.ValidatePySparkJoins import ValidatePySparkJoins


class SymbolicExecution:

    def open(self) -> OpenPySparkStep:
        return OpenPySparkStep()

    def capture(self) -> CapturePySparkStep:
        return CapturePySparkStep()

    def rewrite(self) -> RewritePySparkStepBody:
        return RewritePySparkStepBody()

    def validate_joins(self) -> ValidatePySparkJoins:
        return ValidatePySparkJoins()

    def validate_comparisons(self) -> ValidatePySparkComparisons:
        return ValidatePySparkComparisons()
