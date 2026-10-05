from dataclasses import dataclass

from structure.plugin.pyspark.symbolic_execution.model.PySparkForeachCapture import PySparkForeachCapture


@dataclass(frozen=True)
class PySparkSinkEffect:
    """Symbolic return value for a step whose result is a declared sink effect."""

    capture: PySparkForeachCapture
