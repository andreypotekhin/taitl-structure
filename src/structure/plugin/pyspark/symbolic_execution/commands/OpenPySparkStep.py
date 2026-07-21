from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import PySparkSymbolicContext


class OpenPySparkStep:

    def __call__(self, *, step: str, capture_special_exprs: bool = False) -> PySparkSymbolicContext:
        return PySparkSymbolicContext(step=step, capture_special_exprs=capture_special_exprs)
