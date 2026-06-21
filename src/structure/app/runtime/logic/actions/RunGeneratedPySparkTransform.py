from __future__ import annotations

import importlib
from types import ModuleType

from structure.app.backend.pyspark.logic.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.dsl.logic.model.transforms.Transform import Transform
from structure.app.runtime.logic.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.app.runtime.logic.model.StructureRuntimeError import StructureRuntimeError


class RunGeneratedPySparkTransform:

    def __call__(
        self,
        invocation: Transform,
        plan: PySparkExecutionPlan,
        *,
        session,
    ) -> object:
        module = self._import_module(invocation, session=session)
        class_name = f"{plan.transform}Generated"
        try:
            generated_class = getattr(module, class_name)
        except AttributeError as error:
            raise self._error(
                invocation,
                session=session,
                problem=f"Generated module {module.__name__} does not define {class_name}.",
            ) from error

        runner = generated_class(spark=session.spark, ctx=session.ctx)
        return runner.run(**invocation._structure_bound_inputs)

    def _import_module(self, invocation: Transform, *, session) -> ModuleType:
        module_name = self._module_name(invocation, generated_package=session.generated_package)
        try:
            return importlib.import_module(module_name)
        except ImportError as error:
            raise self._error(
                invocation,
                session=session,
                problem=f"Structure could not import generated module {module_name}.",
            ) from error

    def _module_name(self, invocation: Transform, *, generated_package: str) -> str:
        source = type(invocation).__module__
        name = source.rsplit(".", 1)[1]
        return f"{generated_package}.pyspark.transforms.{name}"

    def _error(self, invocation: Transform, *, session, problem: str) -> StructureRuntimeError:
        transform = f"{type(invocation).__module__}.{type(invocation).__name__}"
        diagnostic = RuntimeDiagnostic(
            code="GEN-E0902",
            title="Generated transform is not importable",
            transform=transform,
            execution_mode=session.execution_mode,
            target_backend=session.target_backend,
            problem=problem,
            use=(
                "Run `structure compile`, ensure the generated source root is importable, "
                "or set execution_mode = \"online\"."
            ),
            docs="docs/specifications/OnlineExecution.md",
        )
        return StructureRuntimeError(diagnostic)


run_generated_pyspark_transform = RunGeneratedPySparkTransform()
