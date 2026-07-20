from structure.platform.pyspark.symbolic_execution.commands.CapturePySparkStep import CapturePySparkStep
from structure.platform.pyspark.symbolic_execution.commands.OpenPySparkStep import OpenPySparkStep


class SymbolicExecution:

    def open(self) -> OpenPySparkStep:
        return OpenPySparkStep()

    def capture(self) -> CapturePySparkStep:
        return CapturePySparkStep()
