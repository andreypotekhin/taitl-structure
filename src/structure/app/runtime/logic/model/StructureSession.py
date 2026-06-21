from __future__ import annotations

from collections.abc import Callable

from structure.app.backend.pyspark.logic.actions.LowerPySparkPlan import lower_pyspark_plan
from structure.app.dsl.logic.actions.CompileTransform import compile_transform
from structure.app.dsl.logic.model.transforms.Transform import Transform
from structure.app.runtime.logic.actions.BuildTransformSchemas import build_transform_schemas
from structure.app.runtime.logic.actions.RunGeneratedPySparkTransform import run_generated_pyspark_transform
from structure.app.runtime.logic.actions.RunOnlinePySparkTransform import run_online_pyspark_transform
from structure.app.runtime.logic.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.app.runtime.logic.model.StructureRuntimeError import StructureRuntimeError


class StructureSession:

    def __init__(
        self,
        *,
        spark=None,
        ctx=None,
        execution_mode: str = "online",
        target_backend: str = "pyspark",
        generated_package: str = "structure_generated",
        schema_types=None,
        online_executor: Callable[..., object] | None = None,
    ) -> None:
        self.spark = spark
        self.ctx = ctx
        self.execution_mode = execution_mode
        self.target_backend = target_backend
        self.generated_package = generated_package
        self.schema_types = schema_types
        self.online_executor = online_executor

    def run(self, invocation: Transform) -> object:
        plan = lower_pyspark_plan(compile_transform(type(invocation)))
        self._validate_inputs(invocation)
        invocation.schemas = build_transform_schemas(plan, types=self.schema_types)

        if self.execution_mode == "online":
            return run_online_pyspark_transform(invocation, plan, session=self)
        if self.execution_mode == "generated":
            return run_generated_pyspark_transform(invocation, plan, session=self)
        raise self._invalid_mode(invocation)

    def _validate_inputs(self, invocation: Transform) -> None:
        declared = set(type(invocation)._structure_inputs)
        bound = set(invocation._structure_bound_inputs)
        missing = sorted(declared - bound)
        if missing:
            raise self._input_error(
                invocation,
                code="ONLINE-E1201",
                title="Transform input is missing",
                problem=f"Missing declared transform input(s): {', '.join(missing)}.",
                use="Pass every declared input DataFrame to the transform invocation before calling run(session).",
                context={"inputs": ", ".join(missing)},
            )

    def _invalid_mode(self, invocation: Transform) -> StructureRuntimeError:
        return self._input_error(
            invocation,
            code="ONLINE-E1203",
            title="Execution mode is unsupported",
            problem=f"Unsupported execution mode: {self.execution_mode}.",
            use="Use execution_mode = \"online\" or execution_mode = \"generated\".",
            context={"execution_mode": self.execution_mode},
        )

    def _input_error(
        self,
        invocation: Transform,
        *,
        code: str,
        title: str,
        problem: str,
        use: str,
        context: dict[str, str],
    ) -> StructureRuntimeError:
        transform = f"{type(invocation).__module__}.{type(invocation).__name__}"
        diagnostic = RuntimeDiagnostic(
            code=code,
            title=title,
            transform=transform,
            execution_mode=self.execution_mode,
            target_backend=self.target_backend,
            problem=problem,
            use=use,
            docs="docs/specifications/OnlineExecution.md",
            context=context,
        )
        return StructureRuntimeError(diagnostic)
