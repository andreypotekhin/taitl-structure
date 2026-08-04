from __future__ import annotations

import importlib
from types import ModuleType

from structure.dsl import Transform
from structure.plugin.api.v1.model import RuntimeDiagnostic, StructureRuntimeError, TransformResult
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.execution.logic.SparkConnectRuntimeDiagnostics import spark_connect_runtime_error
from structure.plugin.pyspark.GeneratedPySparkTransformModule import (
    generated_pyspark_transform_module,
    legacy_generated_pyspark_transform_module,
)


class RunGeneratedPySparkTransform:

    @property
    def _options(self):
        from structure.plugin.pyspark.api.PySpark import PySpark

        return PySpark.render.options()

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
        source = type(invocation).__module__
        module_names = (
            generated_pyspark_transform_module(source, generated_package=session.generated_package),
            legacy_generated_pyspark_transform_module(source, generated_package=session.generated_package),
        )
        storage = getattr(session, "storage", None)
        errors = []
        for module_name in dict.fromkeys(module_names):
            try:
                if storage is not None and hasattr(storage, "import_module"):
                    return storage.import_module(module_name)
                return importlib.import_module(module_name)
            except (ImportError, KeyError) as error:
                errors.append(error)
        location = "configured storage" if storage is not None and hasattr(storage, "import_module") else "the import path"
        problem = f"Structure could not import generated modules {', '.join(module_names)} from {location}."
        raise self._error(invocation, session=session, problem=problem) from errors[-1]

    def _module_name(self, invocation: Transform, *, generated_package: str) -> str:
        source = type(invocation).__module__
        return generated_pyspark_transform_module(source, generated_package=generated_package)

    def _verify_fingerprint(self, module, *, source_transform: str, expected: str | None, invocation, session) -> None:
        if expected is None:
            return
        fingerprints = getattr(module, "STRUCTURE_ARTIFACT_FINGERPRINTS", {})
        transform = source_transform.rsplit(".", 1)[1]
        if fingerprints.get(source_transform, fingerprints.get(transform)) == expected:
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
            target=session.target,
            problem=problem,
            use=(
                "Run `structure compile`, ensure the generated source root is importable, "
                "or switch to direct execution with execution_mode = \"online\"."
            ),
            docs="docs/background/Execution.back.md",
            context={
                "target_profile": str(getattr(session, "plugin_options", {}).get("profile", ">=3.5,<4.1")),
                "target_variant": str(getattr(session, "plugin_options", {}).get("variant", "ordinary")),
            },
        )
        return StructureRuntimeError(diagnostic)


run_generated_pyspark_transform = RunGeneratedPySparkTransform()
