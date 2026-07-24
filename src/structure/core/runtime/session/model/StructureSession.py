from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from structure.core.compiler.artifacts.api.Artifacts import Artifacts
from structure.core.compiler.artifacts.model import CompiledArtifactPool, CompiledTransform, CompilerOptions
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.core.plugins.api.Plugin import Plugin
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.core.runtime.session.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.core.runtime.session.model.StructureRuntimeError import StructureRuntimeError
from structure.core.runtime.session.model.TransformResult import TransformResult
from structure.core.sources.api import Sources
from structure.core.sources.model.CompiledSources import CompiledSources
from structure.core.sources.model.SourceTransformAddress import SourceTransformAddress
from structure.core.sources.model.StructureSources import StructureSources
from structure.plugin.api.v1.model import ExecutionRequest


class StructureSession:

    def __init__(
        self,
        *,
        spark=None,
        runtime=None,
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
        if spark is not None and runtime is not None:
            raise ValueError("Pass either runtime= or the legacy spark= argument, not both.")
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
        self.runtime = runtime if runtime is not None else spark
        self.spark = self.runtime
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
        self._source_transforms: dict[SourceTransformAddress, list[type[Transform]]] = {}

    def run(self, invocation: Transform | None = None, *, transform=None, **inputs) -> TransformResult:
        if transform is not None:
            if invocation is not None:
                raise ValueError("Pass either a transform invocation or transform=, not both.")
            return self._run_source(transform, inputs)
        if invocation is None:
            raise TypeError(
                "StructureSession.run(...) requires a transform invocation or transform=python.module:Class."
            )
        if self._target(invocation) != self.target_backend:
            return self._run_plugin(invocation)
        artifact = self._compiled(invocation)
        self._validate_inputs(invocation, artifact)
        schemas = artifact.schemas
        if schemas is None:
            raise RuntimeError("Runtime execution requires materialized transform schemas")

        plugin = Plugin.registry().select(self.target_backend)
        if plugin.api.executor is None:
            raise self._invalid_mode(invocation)
        result = plugin.api.executor.execute(
            ExecutionRequest(
                payload=artifact.payload,
                runtime=self,
                invocation=invocation,
                mode=self.execution_mode,
                semantic_fingerprint=artifact.semantic_fingerprint,
            )
        )
        if not isinstance(result, TransformResult):
            raise TypeError(f"Plugin {self.target_backend!r} returned an invalid execution result.")
        return result._structure_with_schema(schemas.outputs, aliases=schemas.output_aliases)

    def _run_plugin(self, invocation: Transform) -> TransformResult:
        configuration = self._plugin_configuration()
        artifact = Artifacts().plugin(Plugin.registry())(type(invocation), configuration=configuration)
        plugin = Plugin.registry().select(artifact.plugin, disabled_distributions=configuration.disabled_distributions)
        if plugin.api.executor is None:
            raise self._invalid_mode(invocation)
        value = plugin.api.executor.execute(
            ExecutionRequest(
                payload=artifact.payload,
                runtime=self._plugin_runtime(invocation),
                invocation=invocation,
                mode=self.execution_mode,
                semantic_fingerprint=artifact.fingerprint,
            )
        )
        if isinstance(value, TransformResult):
            return value
        outputs = tuple(type(invocation)._structure_outputs)
        if len(outputs) > 1:
            raise TypeError(
                f"Plugin {artifact.plugin!r} returned one value for {len(outputs)} transform outputs. "
                "Return TransformResult for a multi-output transform."
            )
        return TransformResult({outputs[0] if outputs else "result": value}, single=True)

    def _target(self, invocation: Transform | TransformPipeline) -> str:
        return Plugin.resolve_target()(invocation, configuration=self._plugin_configuration())

    def _plugin_configuration(self) -> PluginConfiguration:
        return PluginConfiguration(
            default=self.target_backend,
            disabled_distributions=frozenset(),
            plugin_options=None,
            plugins=self.config.plugin_options,
        )

    def _plugin_runtime(self, invocation: Transform) -> object:
        inputs = invocation._structure_bound_inputs
        if not inputs:
            return self.runtime
        return inputs

    def _compiled(self, invocation: Transform) -> CompiledTransform:
        return self.compile(invocation if isinstance(invocation, TransformPipeline) else type(invocation))

    def compile(self, transform_or_pipeline: type[Transform] | TransformPipeline | StructureSources):
        if isinstance(transform_or_pipeline, StructureSources):
            compiled = Artifacts().sources()(
                transform_or_pipeline,
                compile_one=lambda subject: self.artifacts.get_or_compile(
                    subject,
                    options=self.compiler_options,
                    schema_types=self.schema_types,
                ),
            )
            self._register_sources(compiled)
            return compiled
        return self.artifacts.get_or_compile(
            transform_or_pipeline,
            options=self.compiler_options,
            schema_types=self.schema_types,
        )

    def load(self, artifact: CompiledTransform | CompiledSources) -> None:
        if isinstance(artifact, CompiledSources):
            self._register_sources(artifact)
            for item in artifact.values():
                self.artifacts.load(item)
            return
        self.artifacts.load(artifact)

    def load_many(self, artifacts) -> object:
        for artifact in artifacts:
            self.load(artifact)
        return self.artifacts.status()

    def clear_compiled(self) -> None:
        self.artifacts.clear()

    def cache_status(self):
        return self.artifacts.status()

    def _register_sources(self, compiled: CompiledSources) -> None:
        transforms = Sources().discover()(compiled.sources)
        for address, transform in transforms.items():
            registered = self._source_transforms.setdefault(address, [])
            if transform not in registered:
                registered.append(transform)

    def _run_source(self, transform, inputs: dict[str, object]) -> TransformResult:
        address = SourceTransformAddress.parse(transform)
        candidates = self._source_transforms.get(address, [])
        if not candidates:
            raise ValueError(f"No compiled source transform {address}. Call session.compile(sources) first.")
        if len(candidates) > 1:
            raise ValueError(f"Source transform {address} is ambiguous across compiled source sets.")
        return self.run(candidates[0](**inputs))

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
            docs="docs/background/Execution.back.md",
            context=context,
        )
        return StructureRuntimeError(diagnostic)
