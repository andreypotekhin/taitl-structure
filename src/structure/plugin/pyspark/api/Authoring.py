from __future__ import annotations

from dataclasses import dataclass

from structure.plugin.api.v1 import StepAuthoringCapture, StepAuthoringRequest
from structure.plugin.pyspark.api.PySpark import PySpark
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


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
        self._capture_pending = False

    def __enter__(self) -> PySparkStepSession:
        self._context.__enter__()
        self._context.default_project_source = self._arguments[0]
        self._context.register_current_scope(self._request.inputs[0].scope)
        for binding, argument in zip(self._request.inputs[1:], self._arguments[1:], strict=True):
            self._context.register_relation_scope(binding.scope, argument)
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if exc_type is not None:
            self._context.__exit__(exc_type, exc, traceback)
            return None
        self._capture_pending = True
        return None

    def arguments(self) -> tuple[object, ...]:
        return self._arguments

    def capture(self, value: object) -> StepAuthoringCapture:
        try:
            return StepAuthoringCapture(
                body=PySpark.symbolic_execution.capture()(value, context=self._context, request=self._request)
            )
        finally:
            if self._capture_pending:
                self._context.__exit__(None, None, None)
                self._capture_pending = False

    def _build_arguments(self) -> tuple[object, ...]:
        arguments: list[object] = []
        for binding in self._request.inputs:
            schema = binding.schema
            if not isinstance(schema, type):
                raise TypeError(f"PLUGIN-E2708: PySpark step {self._request.name!r} has an invalid schema binding.")
            argument = (
                RowScope(name=binding.scope, schema=schema)
                if binding.driving
                else InputScope(name=binding.scope, schema=schema, source=binding.source)
            )
            arguments.append(argument)
        if not arguments:
            return ()
        return tuple(arguments)
