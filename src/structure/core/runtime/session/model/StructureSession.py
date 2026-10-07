from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from importlib import import_module
from pathlib import Path
from typing import Any, Literal

from structure.core.compiler.artifacts.api.Artifacts import Artifacts
from structure.core.compiler.artifacts.model import CompiledArtifactPool, CompiledTransform, CompilerOptions
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.core.plugins.api.Plugin import Plugin
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.core.runtime.session.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.core.runtime.session.model.SinkResult import SinkResult
from structure.core.runtime.session.model.StateBudgetGuard import StateBudgetGuard
from structure.core.runtime.session.model.StructureRuntimeError import StructureRuntimeError
from structure.core.runtime.session.model.TransformResult import TransformResult
from structure.core.sources.api import Sources
from structure.core.sources.model.CompiledSources import CompiledSources
from structure.core.sources.model.SourceTransformAddress import SourceTransformAddress
from structure.core.sources.model.StructureSources import StructureSources
from structure.plugin.api.v1.model import ExecutionRequest


class StructureSession:
    runtime: Any
    spark: Any
    ctx: Any
    config: StructureConfig
    execution_mode: str
    target: str
    plugin_options: Mapping[str, object]
    generated_package: str
    schema_types: Any
    online_executor: Callable[..., object] | None
    storage: Any
    compiler_options: CompilerOptions
    artifacts: CompiledArtifactPool
    _source_transforms: dict[SourceTransformAddress, list[type[Transform]]]
    _closed: bool

    def __init__(
        self,
        parent: StructureSession | None = None,
        *,
        spark=None,
        runtime=None,
        ctx=None,
        config: StructureConfig | None = None,
        project_root: Path | str | None = None,
        execution_mode: str | None = None,
        target: str | None = None,
        generated_package: str | None = None,
        schema_types: Any = None,
        online_executor: Callable[..., object] | None = None,
        storage: Any = None,
        artifacts: CompiledArtifactPool | None = None,
    ) -> None:
        if parent is not None:
            if parent._closed:
                raise ValueError("Cannot spawn a StructureSession from a closed parent session.")
            if any(
                value is not None
                for value in (
                    spark,
                    runtime,
                    ctx,
                    config,
                    project_root,
                    execution_mode,
                    target,
                    generated_package,
                    schema_types,
                    online_executor,
                    storage,
                    artifacts,
                )
            ):
                raise ValueError("StructureSession(parent) cannot be combined with constructor overrides.")
            self.runtime = parent.runtime
            self.spark = parent.spark
            self.ctx = parent.ctx
            self.config = parent.config
            self.execution_mode = parent.execution_mode
            self.target = parent.target
            self.plugin_options = dict(parent.plugin_options)
            self.generated_package = parent.generated_package
            self.schema_types = parent.schema_types
            self.online_executor = parent.online_executor
            self.storage = parent.storage
            self.compiler_options = parent.compiler_options
            self.artifacts = parent.artifacts
            self._source_transforms = {address: list(transforms) for address, transforms in parent._source_transforms.items()}
            self._closed = False
            return
        if spark is not None and runtime is not None:
            raise ValueError("Pass either runtime= or the legacy spark= argument, not both.")
        overrides: dict[str, object] = {
            "execution_mode": execution_mode,
            "generated_package": generated_package,
        }
        if target is not None:
            overrides["plugin"] = {"default": target}
        supplied_overrides = {key: value for key, value in overrides.items() if value is not None}
        if config is not None and (project_root is not None or supplied_overrides):
            raise ValueError(
                "Pass either config=StructureConfig.resolve(...), "
                "or pass project_root/config override fields, not both."
            )

        resolved = config or StructureConfig.resolve(project_root=project_root, overrides=supplied_overrides)
        self.runtime = runtime if runtime is not None else spark
        resolved = self._resolve_runtime_temporal_settings(resolved)
        self.spark = self.runtime
        self.ctx = ctx
        self.config = resolved
        self.execution_mode = resolved.execution_mode
        self.target = resolved.target
        self.plugin_options = dict(resolved.plugin_options.get(self.target, {}))
        self.generated_package = resolved.generated_package
        self.schema_types = schema_types
        self.online_executor = online_executor
        self.storage = storage
        self.compiler_options = CompilerOptions.from_config(resolved, schema_types=schema_types)
        self.artifacts = artifacts or CompiledArtifactPool()
        self._source_transforms: dict[SourceTransformAddress, list[type[Transform]]] = {}
        self._closed = False

    def __enter__(self) -> StructureSession:
        return self

    def __exit__(self, exc_type, exc, traceback) -> Literal[False]:
        self.close()
        return False

    def close(self) -> None:
        """Drop temporary views created by Structure without stopping Spark."""
        if self.runtime is None or self._closed:
            return
        try:
            from structure.plugin.pyspark.execution.logic.PlanBoundary import close_plan_boundaries

            close_plan_boundaries(self.runtime, owner=self)
        except ImportError:
            return
        finally:
            self._closed = True

    def spawn(self) -> StructureSession:
        """Create a child session with this session's runtime configuration."""
        return StructureSession(self)

    def run(self, invocation: Transform | None = None, *, transform=None, **inputs) -> TransformResult:
        if transform is not None:
            if invocation is not None:
                raise ValueError("Pass either a transform invocation or transform=, not both.")
            return self._run_source(transform, inputs)
        if invocation is None:
            raise TypeError(
                "StructureSession.run(...) requires a transform invocation or transform=python.module:Class."
            )
        if self._target(invocation) != self.target:
            return self._run_plugin(invocation)
        artifact = self._compiled(invocation)
        self._validate_inputs(invocation, artifact)
        schemas = artifact.schemas
        if schemas is None:
            raise RuntimeError("Runtime execution requires materialized transform schemas")

        plugin = Plugin.registry().select(self.target)
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
            raise TypeError(f"Plugin {self.target!r} returned an invalid execution result.")
        result._structure_with_schema(schemas.outputs, aliases=schemas.output_aliases)
        result._structure_with_state_budget(self._state_budget_policy(artifact.payload))
        if artifact.transform_plan.sinks:
            sink_results = {}
            for sink in artifact.transform_plan.sinks:
                if sink.sink_module is None or sink.sink_qualname is None:
                    raise TypeError(f"Sink plan {sink.name!r} is missing its importable Schema type.")
                sink_schema: object = import_module(sink.sink_module)
                for part in sink.sink_qualname.split("."):
                    sink_schema = getattr(sink_schema, part)
                if not isinstance(sink_schema, type):
                    raise TypeError(f"Declared sink Schema {sink.sink_qualname!r} is no longer a class.")
                sink_results[sink.name] = SinkResult(
                    dataframe=result[sink.output],
                    schema=sink_schema,
                    output=sink.output,
                    kind=sink.kind,
                    name=sink.name,
                )
            result._structure_with_sinks(sink_results)
        return result

    def run_batch(self, handoff: SinkResult, invocation: Transform) -> TransformResult:
        """Run a fully constructed batch transform for a declared batch sink."""
        if not isinstance(handoff, SinkResult) or handoff.kind != "batch":
            raise TypeError("run_batch(handoff, invocation) requires a foreach_batch handoff.")
        if not isinstance(invocation, Transform):
            raise TypeError("run_batch(handoff, invocation) requires a constructed Transform invocation.")
        handoff_frame = handoff.dataframe
        if not bool(getattr(handoff_frame, "isStreaming", False)):
            raise ValueError("run_batch requires a handoff from a streaming output.")

        artifact = self._compiled(invocation)
        if artifact.schemas is None:
            raise RuntimeError("Batch transform execution requires materialized transform schemas.")
        inputs = artifact.transform_plan.inputs
        if any(input.streaming for input in inputs):
            raise ValueError("run_batch requires batch transform inputs; remove streaming=True from the invocation.")
        matching_inputs = [input for input in inputs if input.schema is handoff.schema]
        if len(matching_inputs) != 1:
            raise ValueError(
                "run_batch requires exactly one batch input whose Structure Schema matches the sink declaration."
            )
        batch_input = matching_inputs[0]
        batch_frame = invocation._structure_bound_inputs.get(batch_input.name)
        if batch_frame is None:
            raise ValueError(f"Batch transform input {batch_input.name!r} is not bound.")
        if bool(getattr(batch_frame, "isStreaming", False)):
            raise ValueError(f"Batch transform input {batch_input.name!r} is a streaming DataFrame.")
        source_schema = getattr(handoff_frame, "schema", None)
        batch_schema = getattr(batch_frame, "schema", None)
        if source_schema is not None and batch_schema is not None and source_schema != batch_schema:
            raise ValueError(
                f"Batch input {batch_input.name!r} has a different Spark schema from the sink handoff output."
            )
        for input in inputs:
            frame = invocation._structure_bound_inputs.get(input.name)
            if frame is not None and bool(getattr(frame, "isStreaming", False)):
                raise ValueError(f"Batch transform input {input.name!r} is a streaming DataFrame.")

        return self.run(invocation)

    def state_budget_guard(self, result: TransformResult) -> StateBudgetGuard:
        """Create a guard that the caller may attach to its own streaming query."""
        if not isinstance(result, TransformResult):
            raise TypeError("state_budget_guard(result) requires a Structure TransformResult")
        return StateBudgetGuard(runtime=self.runtime, policy=result.state_budget)

    def _state_budget_policy(self, payload: object) -> dict[str, object]:
        from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan

        if not isinstance(payload, PySparkExecutionPlan):
            return {}
        memory = payload.transform_memory_budget
        source = getattr(memory, "memory_source", None) or payload.state_budget_memory_source
        fallback = getattr(memory, "fallback_mb", None)
        if fallback is None:
            fallback = payload.state_budget_fallback_mb
        active_mb = self._active_rocksdb_limit()
        warning = None
        resolved_mb = None
        resolved_source = None
        if source == "spark":
            if active_mb is None:
                raise RuntimeError(
                    "State memory budget requested memory_source='spark', but no active bounded RocksDB pool is "
                    "configured. Set the RocksDB provider, enable spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage, "
                    "and set spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB before compiling the query."
                )
            resolved_mb = active_mb
            resolved_source = "spark"
        elif source == "prefer_spark":
            if active_mb is not None:
                resolved_mb = active_mb
                resolved_source = "spark"
            else:
                resolved_mb = fallback
                resolved_source = "fallback"
                warning = "Fallback memory is a declaration only; Spark runtime memory enforcement is not active."
        operators = []
        for step in payload.steps:
            for index, operation in enumerate(step.operations):
                stateful = (
                    operation.kind == "drop_duplicates"
                    or operation.aggregate is not None
                    or operation.stateful_transform is not None
                    or operation.join is not None
                )
                if not stateful:
                    continue
                budget = operation.state_budget
                operators.append(
                    {
                        "id": f"{step.ordinal}:{index}",
                        "step": step.name,
                        "operation": operation.kind,
                        "max_rows": None if budget is None else budget.max_rows,
                        "max_state_bytes": None if budget is None else budget.max_state_bytes,
                    }
                )
        return {
            "checking": payload.state_budget_checking,
            "memory_source": resolved_source or source,
            "memory_mb": resolved_mb,
            "warning": warning,
            "track_rows": self._track_state_rows(),
            "operators": tuple(operators),
        }

    def _active_rocksdb_limit(self) -> int | None:
        conf = getattr(self.runtime, "conf", None)
        if conf is None:
            return None
        provider = self._conf_value(conf, "spark.sql.streaming.stateStore.providerClass")
        bounded = self._conf_value(conf, "spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage")
        limit = self._conf_value(conf, "spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB")
        if "rocksdb" not in str(provider or "").lower() or str(bounded).lower() != "true":
            return None
        try:
            parsed = int(limit)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    def _track_state_rows(self) -> bool | None:
        conf = getattr(self.runtime, "conf", None)
        if conf is None:
            return None
        value = self._conf_value(conf, "spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows")
        if value is None:
            return True
        return str(value).lower() != "false"

    @staticmethod
    def _conf_value(conf, key: str):
        try:
            return conf.get(key)
        except Exception:
            return None

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
            default=self.target,
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
        return self.compile(invocation)

    def compile(self, transform_or_pipeline: type[Transform] | Transform | TransformPipeline | StructureSources):
        self._check_runtime_temporal_settings()
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

    def _resolve_runtime_temporal_settings(self, config: StructureConfig) -> StructureConfig:
        conf = getattr(self.runtime, "conf", None)
        if conf is None:
            return config
        values = dict(config.spark_sql)
        for name in ("spark.sql.timestampType", "spark.sql.legacy.interval.enabled"):
            raw = conf.get(name, str(values[name]))
            actual = raw.upper() if name.endswith("timestampType") else str(raw).lower() == "true"
            if name.endswith("timestampType") and actual not in {"TIMESTAMP_LTZ", "TIMESTAMP_NTZ"}:
                raise ValueError(f"Unsupported Spark {name}={raw!r}; use TIMESTAMP_LTZ or TIMESTAMP_NTZ.")
            if config.source_map.get(name) != "default" and values[name] != actual:
                raise ValueError(f"Spark {name}={raw!r} conflicts with Structure configuration {values[name]!r}.")
            values[name] = actual
        return replace(config, spark_sql=values)

    def _check_runtime_temporal_settings(self) -> None:
        observed = self._resolve_runtime_temporal_settings(self.config)
        for name in ("spark.sql.timestampType", "spark.sql.legacy.interval.enabled"):
            if observed.spark_sql[name] != self.config.spark_sql[name]:
                raise ValueError(
                    f"Spark {name} changed after StructureSession creation. Create a new session and recompile."
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
        declared = {input.name for input in artifact.transform_plan.inputs}
        bound = set(invocation._structure_bound_inputs)
        optional = {input.name for input in artifact.transform_plan.inputs if input.optional}
        missing = sorted(declared - bound - optional)
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
            target=self.target,
            problem=problem,
            use=use,
            docs="docs/background/Execution.back.md",
            context=context,
        )
        return StructureRuntimeError(diagnostic)
