from __future__ import annotations

from dataclasses import dataclass

from structure.platform.api.v1 import StepAuthoringRequest
from structure.platform.api.v1.model import SymbolicContext
from structure.platform.pyspark.api.PySpark import PySpark
from structure.platform.pyspark.dsl.InputScope import InputScope
from structure.platform.pyspark.dsl.RowScope import RowScope
from structure.platform.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


@dataclass
class Authoring:
    def open_step(self, request: StepAuthoringRequest) -> PySparkStepSession:
        return PySparkStepSession(request)


class PySparkStepSession:
    def __init__(self, request: StepAuthoringRequest) -> None:
        self._request = request
        self._arguments = self._build_arguments()
        self._context = PySpark.symbolic_execution.open()(
            step=request.name,
            capture_special_exprs=request.capture_special_exprs,
        )

    def __enter__(self) -> PySparkStepSession:
        self._context.__enter__()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._context.__exit__(exc_type, exc, traceback)
        return None

    def arguments(self) -> tuple[object, ...]:
        return self._arguments

    def context(self) -> SymbolicContext:
        return self._context

    def capture(self, value: object) -> object:
        return PySparkStepBody(value)

    def _build_arguments(self) -> tuple[object, ...]:
        arguments: list[object] = []
        for binding in self._request.inputs:
            schema = binding.schema
            if not isinstance(schema, type):
                raise TypeError(f"PLATFORM-E2708: PySpark step {self._request.name!r} has an invalid schema binding.")
            argument = (
                RowScope(name=binding.scope, schema=schema)
                if binding.driving
                else InputScope(name=binding.scope, schema=schema, source=binding.source)
            )
            arguments.append(argument)
        if not arguments:
            return ()
        return tuple(arguments)
