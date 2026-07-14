from __future__ import annotations

import importlib
from types import ModuleType

from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.runtime.execution.logic.SparkConnectRuntimeDiagnostics import spark_connect_runtime_error
from structure.app.runtime.session.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.app.runtime.session.model.StructureRuntimeError import StructureRuntimeError
from structure.app.runtime.session.model.TransformResult import TransformResult
from structure.app.target.pyspark.logic.GeneratedCodeOptions import GeneratedCodeOptions
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan


class RunGeneratedPySparkTransform:

    def __init__(self) -> None:
        self._options = GeneratedCodeOptions()

    def __call__(
        self,
        invocation: Transform,
        plan: PySparkExecutionPlan,
        *,
        session,
        semantic_fingerprint: str | None = None,
    ) -> TransformResult:
        module = self._import_module(invocation, session=session)
        class_name = f"{plan.transform}Generated"
        self._verify_fingerprint(
            module,
            source_transform=f"{type(invocation).__module__}.{type(invocation).__name__}",
            expected=semantic_fingerprint,
            invocation=invocation,
            session=session,
        )
        try:
            generated_class = getattr(module, class_name)
        except AttributeError as error:
            raise self._error(
                invocation,
                session=session,
                problem=f"Generated module {module.__name__} does not define {class_name}.",
            ) from error

        try:
            result = self._run(generated_class, invocation, session=session)
        except Exception as error:
            boundary = spark_connect_runtime_error(
                invocation,
                session=session,
                error=error,
                surface="generated transform or hook code",
            )
            if boundary is not None:
                raise boundary from error
            raise
        if isinstance(result, TransformResult):
            return result
        if hasattr(result, "as_dict"):
            return TransformResult(
                result.as_dict(),
                single=len(plan.outputs) == 1,
                aliases=self._output_aliases(plan),
            )
        return self._result(plan, result)

    def _run(self, generated_class, invocation: Transform, *, session):
        inputs = invocation._structure_bound_inputs
        if self._options.enabled(session.config.generated_code_options, "mirror_methods"):
            return generated_class(spark=session.spark, ctx=session.ctx, **inputs).run()
        return generated_class(spark=session.spark, ctx=session.ctx).run(**inputs)

    def _result(self, plan: PySparkExecutionPlan, df) -> TransformResult:
        if len(plan.outputs) == 1:
            return TransformResult({plan.outputs[0].name: df}, single=True, aliases=self._output_aliases(plan))
        raise TypeError("Generated multi-output transforms must return TransformResult")

    def _output_aliases(self, plan: PySparkExecutionPlan) -> dict[str, tuple[str, ...]]:
        return {output.name: output.aliases for output in plan.outputs if output.aliases}

    def _import_module(self, invocation: Transform, *, session) -> ModuleType:
        module_name = self._module_name(invocation, generated_package=session.generated_package)
        storage = getattr(session, "storage", None)
        if storage is not None and hasattr(storage, "import_module"):
            try:
                return storage.import_module(module_name)
            except (ImportError, KeyError) as error:
                raise self._error(
                    invocation,
                    session=session,
                    problem=f"Structure could not import generated module {module_name} from configured storage.",
                ) from error
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

    def _verify_fingerprint(self, module, *, source_transform: str, expected: str | None, invocation, session) -> None:
        if expected is None:
            return
        fingerprints = getattr(module, "STRUCTURE_ARTIFACT_FINGERPRINTS", {})
        if fingerprints.get(source_transform) == expected:
            return
        raise self._error(
            invocation,
            session=session,
            code="GEN-E0901",
            title="Generated output is stale",
            problem=(
                f"Generated module {module.__name__} was not rendered from the current compiled transform artifact."
            ),
        )

    def _error(
        self,
        invocation: Transform,
        *,
        session,
        problem: str,
        code: str = "GEN-E0902",
        title: str = "Generated transform is not importable",
    ) -> StructureRuntimeError:
        transform = f"{type(invocation).__module__}.{type(invocation).__name__}"
        diagnostic = RuntimeDiagnostic(
            code=code,
            title=title,
            transform=transform,
            execution_mode=session.execution_mode,
            target_backend=session.target_backend,
            target_profile=getattr(session, "target_profile", ">=3.5,<4.1"),
            target_variant=getattr(session, "target_variant", "ordinary"),
            problem=problem,
            use=(
                "Run `structure compile`, ensure the generated source root is importable, "
                "or switch to direct execution with execution_mode = \"online\"."
            ),
            docs="docs/background/Execution.back.md",
        )
        return StructureRuntimeError(diagnostic)


run_generated_pyspark_transform = RunGeneratedPySparkTransform()
