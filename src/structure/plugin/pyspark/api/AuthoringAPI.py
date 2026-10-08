from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import cast

from structure.plugin.api.v1 import AuthoringAPI as AuthoringAPIV1
from structure.plugin.api.v1 import StepAuthoringCapture, StepAuthoringRequest, StepAuthoringResult
from structure.plugin.pyspark.api.PySpark import PySpark
from structure.plugin.pyspark.delta.operations import DeltaScope
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.iceberg.operations import IcebergScope
from structure.plugin.pyspark.symbolic_execution.model.PySparkSinkEffect import PySparkSinkEffect
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


@dataclass
class AuthoringAPI(AuthoringAPIV1):
    def open_step(self, request: StepAuthoringRequest) -> PySparkStepSession:
        return PySparkStepSession(request)

    def result_arguments(self, results: tuple[StepAuthoringResult, ...]) -> tuple[object, ...]:
        from structure.plugin.pyspark.delta.operations import DeltaScope
        from structure.plugin.pyspark.iceberg.operations import IcebergScope

        values = []
        for result in results:
            schema = cast(type, result.schema)
            value: RowScope
            if result.binding in {"delta", "delta_table", "delta_input", "delta_output"}:
                value = DeltaScope(
                    name=schema.__name__, schema=schema, source=result.frame,
                    binding="delta_table" if result.binding == "delta_table" else "delta_input",
                )
                value._structure_table_source = result.table_source
            elif result.binding in {"iceberg", "iceberg_table", "iceberg_input", "iceberg_output"}:
                value = IcebergScope(
                    name=schema.__name__, schema=schema, source=result.frame,
                    binding="iceberg_table" if result.binding == "iceberg_table" else "iceberg_input",
                )
                value._structure_table_source = result.table_source
            else:
                value = RowScope(name=schema.__name__, schema=schema)
            values.append(value)
        return tuple(values)

    def rewrite_body(self, body: object, *, frames: Mapping[str, str]) -> object:
        return PySpark.symbolic_execution.rewrite()(body, frames=frames)


class PySparkStepSession:
    def __init__(self, request: StepAuthoringRequest) -> None:
        self._request = request
        self._arguments = self._build_arguments()
        self._context = PySpark.symbolic_execution.open()(
            step=request.name,
            capture_special_exprs=request.capture_special_exprs,
            step_output_schema=(request.results[0].schema if len(request.results) == 1 else None),
        )
        self._capture_pending = False
        self._forwarded_table_scope: object | None = None

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
        try:
            PySpark.symbolic_execution.validate_comparisons()(self._context.filters, request=self._request)
            body = PySparkStepBody(value=None, joins=tuple(self._context.joins))
            return PySpark.symbolic_execution.validate_joins()(body, request=self._request)
        except BaseException:
            self._close_context()
            raise

    def capture(self, value: object) -> StepAuthoringCapture:
        try:
            if self._forwarded_table_scope is not None and value is None:
                value = self._forwarded_table_scope
            body = PySpark.symbolic_execution.capture()(value, context=self._context, request=self._request)
            table_sources = tuple(
                (result.lane, mutation.target)
                for result in self._request.results
                if result.binding.startswith(("delta", "iceberg"))
                for mutation in body.delta_mutations
                if mutation.output == result.lane or (mutation.output is None and mutation.target == result.lane)
            )
            if body.table_forward and body.table_source is not None and self._request.results:
                table_sources = (*table_sources, (self._request.results[0].lane, body.table_source))
            return StepAuthoringCapture(
                body=body,
                diagnostics=(),
                sinks=body.sinks,
                sink_effect=isinstance(body.value, PySparkSinkEffect),
                table_sources=table_sources,
                effect=bool(
                    self._request.effect
                    or any(
                        mutation.kind not in {
                            "delta_snapshot", "delta_changes", "delta_history", "delta_detail",
                            "iceberg_snapshot", "iceberg_history", "iceberg_snapshots", "iceberg_metadata",
                        }
                        for mutation in body.delta_mutations
                    )
                ),
            )
        finally:
            if self._capture_pending:
                self._close_context()
                self._capture_pending = False

    def forward_parent_table(self, *, table_source: object) -> None:
        if not isinstance(table_source, str):
            raise TypeError("A delegated table effect must resolve to one caller-bound table")
        self._forwarded_table_scope = next(
            (
                argument
                for argument in self._arguments
                if getattr(argument, "_structure_table_source", None) == table_source
            ),
            None,
        )
        if self._forwarded_table_scope is None:
            raise TypeError("A delegated table effect has no matching local table input")
        self._request = replace(self._request, effect=False)

    def _close_context(self) -> None:
        self._context.__exit__(None, None, None)

    def _build_arguments(self) -> tuple[object, ...]:
        arguments: list[object] = []
        for binding in self._request.inputs:
            schema = binding.schema
            if not isinstance(schema, type):
                raise TypeError(f"PLUGIN-E2708: PySpark step {self._request.name!r} has an invalid schema binding.")
            argument: RowScope
            if binding.binding in {"iceberg", "iceberg_input", "iceberg_table"}:
                argument = IcebergScope(
                    name=binding.scope, schema=schema, source=binding.source,
                    binding="iceberg_input" if binding.binding in {"iceberg", "iceberg_input"} else binding.binding,
                )
                argument._structure_table_source = binding.table_source
            elif binding.binding in {"delta_input", "delta_output", "delta_table", "delta"}:
                argument = DeltaScope(
                    name=binding.scope, schema=schema, source=binding.source,
                    binding="delta_input" if binding.binding in {"delta_input", "delta"} else binding.binding,
                )
                argument._structure_table_source = binding.table_source
            elif binding.driving:
                argument = RowScope(name=binding.scope, schema=schema)
            else:
                argument = InputScope(name=binding.scope, schema=schema, source=binding.source)
            arguments.append(argument)
        if not arguments:
            return ()
        return tuple(arguments)
