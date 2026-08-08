from __future__ import annotations

from pathlib import Path
from typing import Mapping, cast, get_origin, get_type_hints

from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.core.compiler.frontend.commands.CompileTransform import (
    CompileTransform,
    _assigned_outputs,
    _semantic_policies,
)
from structure.core.compiler.frontend.logic.CompilerTransformMember import CompilerTransformMember
from structure.core.compiler.ir.model.StepPlan import StepPlan
from structure.core.compiler.ir.model.StepResultPlan import StepResultPlan
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.lib.cross.errors import Diagnostic
from structure.plugin.api.v1 import TransformMemberOrigin


class AnalyzeTransform(CompileTransform):
    """Collect Core-owned transform structure without evaluating a plugin DSL."""

    def __call__(
        self,
        transform_class: type[Transform] | TransformPipeline,
        *,
        config: StructureConfig | None = None,
        project_root: Path | str | None = None,
        overrides: Mapping[str, object] | None = None,
        **settings: object,
    ) -> TransformPlan:
        if config is not None and (project_root is not None or overrides or settings):
            raise ValueError(
                "Pass either config=StructureConfig.resolve(...), or pass project_root/config override fields, not both."
            )
        merged = dict(overrides or {})
        duplicates = set(merged).intersection(settings)
        if duplicates:
            names = ", ".join(sorted(duplicates))
            raise ValueError(f"Configuration override supplied twice: {names}.")
        merged.update(settings)
        resolved = config or StructureConfig.resolve(project_root=project_root, overrides=merged)
        policy_token = _semantic_policies.set((resolved.allow_output_to_input, resolved.allow_to_reassign_output))
        assigned_token = _assigned_outputs.set(set())
        try:
            return self._analyze(transform_class, config=resolved)
        finally:
            _assigned_outputs.reset(assigned_token)
            _semantic_policies.reset(policy_token)

    def _analyze(
        self, transform_class: type[Transform] | TransformPipeline, *, config: StructureConfig
    ) -> TransformPlan:
        if isinstance(transform_class, TransformPipeline):
            return self._compose_pipeline(transform_class, name="ComposedTransform", config=config)
        if (
            not isinstance(transform_class, type)
            or not issubclass(transform_class, Transform)
            or transform_class is Transform
        ):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class if isinstance(transform_class, type) else None,
                problem=f"{getattr(transform_class, '__name__', transform_class)} is not a Transform subclass.",
                use="Compile a class that inherits from structure.Transform or a pipeline built with invocation.to(...).",
            )
        self._require_module_level_schemas(transform_class)
        pipeline = getattr(transform_class, "_structure_pipeline", None)
        if pipeline is not None:
            return self._compose_pipeline(
                pipeline, name=transform_class.__name__, config=config, wrapper_class=transform_class
            )
        if transform_class._structure_stages:
            self._reject_mixed_stage_members(transform_class)
            return self._compose_graph(transform_class, config=config)
        if not transform_class._structure_outputs:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"{transform_class.__name__} declares no outputs.",
                use="Declare at least one transform result with name = output(Schema).",
            )
        inputs = self._input_collector.collect(transform_class)
        if not inputs:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"{transform_class.__name__} declares no inputs.",
                use="Declare at least one transform input with name = input(Schema).",
            )
        steps, lanes, explicit_outputs, diagnostics = self._structural_steps(transform_class, inputs)
        outputs = self._outputs(transform_class, lanes, explicit_outputs)
        return TransformPlan(
            name=transform_class.__name__,
            inputs=tuple(inputs),
            steps=tuple(steps),
            outputs=tuple(outputs),
            options=transform_class.effective_transform_options(),
            diagnostics=tuple(diagnostics),
        )

    def _compose_pipeline(
        self,
        pipeline: TransformPipeline,
        *,
        name: str,
        config: StructureConfig,
        wrapper_class: type[Transform] | None = None,
    ) -> TransformPlan:
        composition_config = self._composition_config(wrapper_class, config)
        return self._composer(
            pipeline,
            name=name,
            compile_stage=lambda transform_class: self._analyze(transform_class, config=composition_config),
            wrapper_class=wrapper_class,
            allow_stream_to_batch=composition_config.allow_stream_to_batch,
            stream_to_batch_policy=composition_config.stream_to_batch_policy,
        )

    def _compose_graph(self, transform_class: type[Transform], *, config: StructureConfig) -> TransformPlan:
        composition_config = self._composition_config(transform_class, config)
        return self._graph_composer(
            transform_class,
            compile_stage=lambda stage: self._analyze(type(stage), config=composition_config),
            allow_stream_to_batch=composition_config.allow_stream_to_batch,
            stream_to_batch_policy=composition_config.stream_to_batch_policy,
        )

    def _structural_steps(self, transform_class, inputs):
        members = self._member_collector.collect(transform_class)
        steps: list[StepPlan] = []
        lanes: dict[str, dict[str, object]] = {}
        explicit_outputs: set[str] = set()
        diagnostics: list[Diagnostic] = []
        pending_raw: list[CompilerTransformMember] = []
        for item in members:
            if getattr(item.member, "_structure_raw", None) is not None:
                if steps:
                    self._attach_raw(transform_class, item, steps, lanes)
                else:
                    pending_raw.append(item)
                continue
            result = self._structural_step(transform_class, item, lanes, inputs, explicit_outputs, ordinal=len(steps))
            if result is None:
                continue
            steps.append(result)
            streaming = self._source_streaming(result.source, lanes, inputs)
            for item in result.results:
                lanes[item.lane] = {
                    "kind": "lane" if item.lane in transform_class._structure_lanes else "output",
                    "schema": item.schema,
                    "source": item.frame,
                    "scope": item.schema.__name__,
                    "streaming": streaming,
                }
            if pending_raw:
                for raw in pending_raw:
                    steps[-1] = self._attach_raw_before(transform_class, raw, steps[-1])
                pending_raw.clear()
        if pending_raw:
            names = ", ".join(item.name for item in pending_raw)
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"{transform_class.__name__} declares @raw method(s) without a following step: {names}.",
                use="Place @raw before or after a schema-returning step method.",
            )
        if not steps:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"{transform_class.__name__} has no public schema-returning step method.",
                use="Add a public instance method with a Schema row parameter and Schema return annotation.",
            )
        return steps, lanes, explicit_outputs, diagnostics

    def _source_streaming(self, source: str, lanes: dict[str, dict[str, object]], inputs) -> bool:
        lane = lanes.get(source)
        if lane is not None:
            return bool(lane.get("streaming", False))
        input_name = source.removeprefix("input:")
        return any(input.name == input_name and input.streaming for input in inputs)

    def _structural_step(self, transform_class, item, lanes, inputs, explicit_outputs, *, ordinal: int):
        member = item.member
        hints = get_type_hints(member)
        output_schemas = self._return_schemas(hints.get("return"))
        if not output_schemas:
            if get_origin(hints.get("return")) is tuple:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=item.name,
                    problem=f"{transform_class.__name__}.{item.name} has an invalid tuple return annotation.",
                    use="Use a fixed tuple of Schema classes, such as tuple[Accepted, Audited].",
                )
            return None
        parameters = self._row_parameters(member, hints)
        metadata = getattr(member, "_structure_output_method", None)
        try:
            bindings = self._input_bindings(transform_class, metadata, lanes, inputs, parameters, member=item.name)
        except StructureCompileError:
            # A schema-returning method after an explicit terminal output may
            # be a direct helper call. Its legality depends on Core's
            # invocation guard, not neutral lane analysis; defer it to the
            # selected authoring pass so that guard can issue its diagnostic.
            declared_schemas = tuple(output.schema for output in transform_class._structure_outputs.values())
            if (
                metadata is None
                and all(schema not in declared_schemas for schema in output_schemas)
                and any(hints.get(parameter.name) is input.schema for parameter in parameters for input in inputs)
            ):
                return None
            raise
        output_lanes = self._output_lanes(
            transform_class,
            metadata,
            lanes,
            output_schemas,
            member=item.name,
            explicit_outputs=explicit_outputs,
            default_lane=bindings[0].lane,
        )
        results = tuple(
            StepResultPlan(
                schema=schema,
                lane=lane,
                frame=lane,
                ordinal=ordinal,
                after_hooks=(),
            )
            for ordinal, (schema, lane) in enumerate(zip(output_schemas, output_lanes, strict=True))
        )
        first = results[0]
        return StepPlan(
            name=item.name,
            input_schema=cast(type, bindings[0].schema),
            output_schema=first.schema,
            source=bindings[0].source,
            source_scope=bindings[0].scope,
            input_lane=bindings[0].lane,
            output_lane=first.lane,
            ordinal=ordinal,
            before_hooks=(),
            after_hooks=(),
            inputs=tuple(bindings),
            results=results,
            options=self._step_options(item.owner, metadata),
            origin=TransformMemberOrigin.of(item.owner, item.name),
            plugin_body=None,
        )
