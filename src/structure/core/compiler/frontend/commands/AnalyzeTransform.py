from __future__ import annotations

from pathlib import Path
from typing import Mapping, cast, get_type_hints

from structure.core.compiler.frontend.commands.CompileTransform import CompileTransform
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
        return self._analyze(transform_class, config=resolved)

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
                use="Compile a class that inherits from structure.Transform or compile a Transform.to(...) pipeline.",
            )
        pipeline = getattr(transform_class, "_structure_pipeline", None)
        if pipeline is not None:
            return self._compose_pipeline(pipeline, name=transform_class.__name__, config=config, wrapper_class=transform_class)
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
            options=dict(transform_class.__dict__.get("_structure_transform_options", {})),
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
        return self._composer(
            pipeline,
            name=name,
            compile_stage=lambda transform_class: self._analyze(transform_class, config=config),
            wrapper_class=wrapper_class,
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
            for item in result.results:
                lanes[item.lane] = {"schema": item.schema, "source": item.frame, "scope": item.schema.__name__}
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

    def _structural_step(self, transform_class, item, lanes, inputs, explicit_outputs, *, ordinal: int):
        member = item.member
        hints = get_type_hints(member)
        output_schemas = self._return_schemas(hints.get("return"))
        if not output_schemas:
            return None
        parameters = self._row_parameters(member, hints)
        metadata = getattr(member, "_structure_output_method", None)
        bindings = self._input_bindings(
            transform_class, metadata, lanes, inputs, parameters, member=item.name
        )
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
