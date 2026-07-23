from structure.plugin.pyspark.symbolic_execution.commands.CapturePySparkStep import CapturePySparkStep
from structure.plugin.pyspark.symbolic_execution.commands.OpenPySparkStep import OpenPySparkStep
from structure.plugin.pyspark.symbolic_execution.commands.RewritePySparkStepBody import RewritePySparkStepBody


class SymbolicExecution:

    def open(self) -> OpenPySparkStep:
        return OpenPySparkStep()

    def capture(self) -> CapturePySparkStep:
        return CapturePySparkStep()

    def rewrite(self) -> RewritePySparkStepBody:
        return RewritePySparkStepBody()
