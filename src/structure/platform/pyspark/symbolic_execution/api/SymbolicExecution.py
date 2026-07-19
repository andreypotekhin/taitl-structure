from structure.platform.pyspark.symbolic_execution.commands.OpenPySparkStep import OpenPySparkStep


class SymbolicExecution:

    def open(self) -> OpenPySparkStep:
        return OpenPySparkStep()
