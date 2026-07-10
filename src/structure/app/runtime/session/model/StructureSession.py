from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from structure.app.compiler.artifacts.commands import CompiledArtifactPool
from structure.app.compiler.artifacts.model import CompiledTransform, CompilerOptions
from structure.app.configuration.model.StructureConfig import StructureConfig
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.app.runtime.execution.api import Execution
from structure.app.runtime.session.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.app.runtime.session.model.StructureRuntimeError import StructureRuntimeError
from structure.app.runtime.session.model.TransformResult import TransformResult


class StructureSession:

    def __init__(
        self,
        *,
        spark=None,
        ctx=None,
        config: StructureConfig | None = None,
        project_root: Path | str | None = None,
        execution_mode: str | None = None,
        target_backend: str | None = None,
        target_profile: str | None = None,
        target_variant: str | None = None,
        generated_package: str | None = None,
        schema_types=None,
        online_executor: Callable[..., object] | None = None,
        storage=None,
        artifacts: CompiledArtifactPool | None = None,
    ) -> None:
        overrides = {
            "execution_mode": execution_mode,
            "target_backend": target_backend,
            "target_profile": target_profile,
            "target_variant": target_variant,
            "generated_package": generated_package,
        }
        supplied_overrides = {key: value for key, value in overrides.items() if value is not None}
        if config is not None and (project_root is not None or supplied_overrides):
            raise ValueError(
                "Pass either config=StructureConfig.resolve(...), "
                "or pass project_root/config override fields, not both."
            )

        resolved = config or StructureConfig.resolve(project_root=project_root, overrides=supplied_overrides)
        self.spark = spark
        self.ctx = ctx
        self.config = resolved
        self.execution_mode = resolved.execution_mode
        self.target_backend = resolved.target_backend
        self.target_profile = resolved.target_profile
        self.target_variant = resolved.target_variant
        self.generated_package = resolved.generated_package
        self.schema_types = schema_types
        self.online_executor = online_executor
        self.storage = storage
        self.compiler_options = CompilerOptions.from_config(resolved, schema_types=schema_types)
        self.artifacts = artifacts or CompiledArtifactPool()

    def run(self, invocation: Transform) -> TransformResult:
        artifact = self._compiled(invocation)
        plan = artifact.pyspark_plan
        self._validate_inputs(invocation, artifact)
        schemas = artifact.schemas
        if schemas is None:
            raise RuntimeError("Runtime execution requires materialized transform schemas")

        if self.execution_mode == "online":
            result = Execution.online.pyspark()(invocation, plan, session=self)
            return result._structure_with_schema(schemas.outputs, aliases=schemas.output_aliases)
        if self.execution_mode == "generated":
            result = Execution.generated.pyspark()(
                invocation,
                plan,
                session=self,
                semantic_fingerprint=artifact.semantic_fingerprint,
            )
            return result._structure_with_schema(schemas.outputs, aliases=schemas.output_aliases)
        raise self._invalid_mode(invocation)

    def _compiled(self, invocation: Transform) -> CompiledTransform:
        return self.compile(invocation if isinstance(invocation, TransformPipeline) else type(invocation))

    def compile(self, transform_or_pipeline: type[Transform] | TransformPipeline) -> CompiledTransform:
        return self.artifacts.get_or_compile(
            transform_or_pipeline,
            options=self.compiler_options,
            schema_types=self.schema_types,
        )

    def load(self, artifact: CompiledTransform) -> None:
        self.artifacts.load(artifact)

    def load_many(self, artifacts) -> object:
        return self.artifacts.load_many(artifacts)

    def clear_compiled(self) -> None:
        self.artifacts.clear()

    def cache_status(self):
        return self.artifacts.status()

    def _validate_inputs(self, invocation: Transform, artifact: CompiledTransform) -> None:
        if isinstance(invocation, TransformPipeline):
            declared = set(input.name for input in artifact.transform_plan.inputs)
        else:
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
            target_profile=self.target_profile,
            target_variant=self.target_variant,
            problem=problem,
            use=use,
            docs="docs/reference/OnlineExecution.md",
            context=context,
        )
        return StructureRuntimeError(diagnostic)
