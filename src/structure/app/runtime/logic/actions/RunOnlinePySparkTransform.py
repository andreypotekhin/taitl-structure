from __future__ import annotations

from structure.app.backend.pyspark.logic.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.dsl.logic.model.transforms.Transform import Transform
from structure.app.runtime.logic.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.app.runtime.logic.model.StructureRuntimeError import StructureRuntimeError


class RunOnlinePySparkTransform:

    def __call__(
        self,
        invocation: Transform,
        plan: PySparkExecutionPlan,
        *,
        session,
    ) -> object:
        if session.online_executor is None:
            raise self._missing_executor(invocation, session=session)
        return session.online_executor(
            plan=plan,
            inputs=invocation._structure_bound_inputs,
            spark=session.spark,
            ctx=session.ctx,
        )

    def _missing_executor(self, invocation: Transform, *, session) -> StructureRuntimeError:
        transform = f"{type(invocation).__module__}.{type(invocation).__name__}"
        diagnostic = RuntimeDiagnostic(
            code="ONLINE-E1202",
            title="Online PySpark runner is not configured",
            transform=transform,
            execution_mode=session.execution_mode,
            target_backend=session.target_backend,
            problem="Structure has no live PySpark executor for this session.",
            use="Pass an online_executor to StructureSession or use execution_mode = \"generated\".",
            docs="docs/specifications/OnlineExecution.md",
        )
        return StructureRuntimeError(diagnostic)


run_online_pyspark_transform = RunOnlinePySparkTransform()
