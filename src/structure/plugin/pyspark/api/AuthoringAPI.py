from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from structure.plugin.api.v1 import AuthoringAPI as AuthoringAPIV1
from structure.plugin.api.v1 import StepAuthoringCapture, StepAuthoringRequest, StepAuthoringResult
from structure.plugin.pyspark.api.PySpark import PySpark
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


@dataclass
class AuthoringAPI(AuthoringAPIV1):
    def open_step(self, request: StepAuthoringRequest) -> PySparkStepSession:
        return PySparkStepSession(request)

    def result_arguments(self, results: tuple[StepAuthoringResult, ...]) -> tuple[object, ...]:
        return tuple(
            RowScope(name=cast(type, result.schema).__name__, schema=cast(type, result.schema)) for result in results
        )

    def rewrite_body(self, body: object, *, frames: Mapping[str, str]) -> object:
        return PySpark.symbolic_execution.rewrite()(body, frames=frames)


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
        self._context.default_project_frame = self._request.inputs[0].source
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

    def validate(self) -> tuple[object, ...]:
        PySpark.symbolic_execution.validate_comparisons()(self._context.filters, request=self._request)
        body = PySparkStepBody(value=None, joins=tuple(self._context.joins))
        return PySpark.symbolic_execution.validate_joins()(body, request=self._request)

    def capture(self, value: object) -> StepAuthoringCapture:
        try:
            body = PySpark.symbolic_execution.capture()(value, context=self._context, request=self._request)
            return StepAuthoringCapture(
                body=body,
                diagnostics=(),
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
