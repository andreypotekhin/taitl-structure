from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import PySparkSymbolicContext


class OpenPySparkStep:

    def __call__(
        self,
        *,
        step: str,
        capture_special_exprs: bool = False,
        step_output_schema=None,
    ) -> PySparkSymbolicContext:
        return PySparkSymbolicContext(
            step=step,
            capture_special_exprs=capture_special_exprs,
            step_output_schema=step_output_schema,
        )
