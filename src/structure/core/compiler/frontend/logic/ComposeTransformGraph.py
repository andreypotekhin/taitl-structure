from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import replace

from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.core.compiler.frontend.logic.ValidateStreamingInputBinding import validate_streaming_input_binding
from structure.core.compiler.ir.model.HookPlan import HookPlan
from structure.core.compiler.ir.model.OutputPlan import OutputPlan
from structure.core.compiler.ir.model.StepInputPlan import StepInputPlan
from structure.core.compiler.ir.model.StepPlan import StepPlan
from structure.core.compiler.ir.model.StepResultPlan import StepResultPlan
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.core.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.core.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.core.dsl.model.transforms.ParameterDeclaration import ParameterDeclaration
from structure.core.dsl.model.transforms.StageDeclaration import StageDeclaration, StageOutputReference
from structure.core.dsl.model.transforms.Transform import Transform
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1 import InputPlan, StreamingBoundaryPlan

CompileStage = Callable[[Transform], TransformPlan]
RewriteBody = Callable[[object, Mapping[str, str]], object]


class ComposeTransformGraph:
    def __call__(
        self,
        wrapper_class: type[Transform],
        *,
        compile_stage: CompileStage,
        rewrite_body: RewriteBody | None = None,
        allow_stream_to_batch: bool = False,
        stream_to_batch_policy: str = "default",
    ) -> TransformPlan:
        rewrite = rewrite_body or (lambda body, _: body)
        stages = tuple(self._stage(wrapper_class, stage) for stage in wrapper_class._structure_stages.values())
        if not stages:
            raise self._error(
                wrapper_class.__name__, "Transform graph has no stages.", "Declare at least one stage(...)."
            )
        if not wrapper_class._structure_outputs:
            raise self._error(
                wrapper_class.__name__,
                "Transform graph declares no outputs.",
                "Declare public outputs with name = output(Schema).",
            )
        stage_plans = tuple(compile_stage(stage.invocation) for stage in stages)
        self._validate_references(wrapper_class, stages)

        stage_by_name = {stage.name: stage for stage in stages}
        inputs, streaming_boundaries = self._inputs(
            wrapper_class,
            stages,
            stage_plans,
            allow_stream_to_batch=allow_stream_to_batch,
            stream_to_batch_policy=stream_to_batch_policy,
        )
        steps, stage_outputs, output_sources = self._rewrite(
            wrapper_class,
            stages,
            stage_plans,
            stage_by_name=stage_by_name,
            rewrite_body=rewrite,
        )
        outputs = self._outputs(wrapper_class, stage_outputs, output_sources, stage_by_name=stage_by_name)
        return TransformPlan(
            name=wrapper_class.__name__,
            inputs=tuple(inputs),
            steps=tuple(steps),
            outputs=tuple(outputs),
            options=Transform.resolve_transform_options(
                wrapper_class.__dict__.get("_structure_transform_options", {}),
                inputs=inputs,
                transform_name=wrapper_class.__name__,
            ),
            diagnostics=tuple(diagnostic for plan in stage_plans for diagnostic in plan.diagnostics),
            streaming_boundaries=tuple(
                boundary for plan in stage_plans for boundary in plan.streaming_boundaries
            ) + tuple(streaming_boundaries),
        )

    def _stage(self, wrapper_class: type[Transform], stage: StageDeclaration) -> StageDeclaration:
        parameters = {
            name: self._parameter(wrapper_class, value)
            for name, value in stage.invocation._structure_bound_parameters.items()
        }
        if parameters == stage.invocation._structure_bound_parameters:
            return stage
        invocation = type(stage.invocation)(**stage.invocation._structure_bound_inputs, **parameters)
        invocation._structure_output_renames = dict(stage.invocation._structure_output_renames)
        return replace(stage, invocation=invocation)

    def _parameter(self, wrapper_class: type[Transform], value: object) -> object:
        if not isinstance(value, ParameterDeclaration):
            return value
        resolved = getattr(wrapper_class, value.name, value)
        return value.default if isinstance(resolved, ParameterDeclaration) else resolved

    def _inputs(
        self,
        wrapper_class: type[Transform],
        stages: tuple[StageDeclaration, ...],
        stage_plans: tuple[TransformPlan, ...],
        *,
        allow_stream_to_batch: bool,
        stream_to_batch_policy: str,
    ) -> tuple[list[InputPlan], list[StreamingBoundaryPlan]]:
        inputs: dict[str, InputPlan] = {}
        streaming_boundaries: list[StreamingBoundaryPlan] = []
        plans = {stage.name: plan for stage, plan in zip(stages, stage_plans, strict=True)}
        effective_outputs: dict[str, dict[str, OutputPlan]] = {}
        for stage, plan in zip(stages, stage_plans, strict=True):
            bound = stage.invocation._structure_bound_inputs
            input_modes: dict[str, bool] = {}
            for input_plan in plan.inputs:
                value = bound.get(input_plan.name)
                if value is None:
                    raise self._error(
                        wrapper_class.__name__,
                        f"{type(stage.invocation).__name__}.{input_plan.name} is not supplied.",
                        "Pass every stage input from a wrapper input or an earlier stage output.",
                    )
                if isinstance(value, StageOutputReference):
                    if input_plan.schema is not value.schema:
                        raise self._error(
                            wrapper_class.__name__,
                            f"{stage.name}.{input_plan.name} expects {self._schema_name(input_plan.schema)}, but {value.stage.name}.{value.name} provides {self._schema_name(value.schema)}.",
                            "Bind stage inputs only to outputs with the same schema.",
                        )
                    producer_plan = plans[value.stage.name]
                    producer = effective_outputs.get(value.stage.name, {}).get(value.name)
                    if producer is None:
                        producer = next(output for output in producer_plan.outputs if output.name == value.name)
                    input_modes[input_plan.name] = bool(producer.streaming)
                    consumer_options = plan.options or {}
                    local_allow = bool(consumer_options.get("allow_stream_to_batch"))
                    explicit_batch = input_plan.streaming_declared or consumer_options.get("streaming") is False
                    defer_boundary = (
                        producer.streaming
                        and not input_plan.streaming
                        and not explicit_batch
                        and stream_to_batch_policy == "default"
                        and not local_allow
                        and not allow_stream_to_batch
                    )
                    violation = None
                    if defer_boundary:
                        streaming_boundaries.append(
                            StreamingBoundaryPlan(
                                producer=value.stage.name,
                                output=value.name,
                                consumer=stage.name,
                                input=input_plan.name,
                            )
                        )
                    else:
                        violation = validate_streaming_input_binding(
                            producer=value.stage.name,
                            output=producer,
                            consumer=type(stage.invocation).__name__,
                            input_plan=input_plan,
                            consumer_options=plan.options,
                            allow_stream_to_batch=allow_stream_to_batch,
                        )
                    if violation is not None:
                        raise self._error(
                            wrapper_class.__name__,
                            violation.problem,
                            violation.use,
                            code="STREAM-E0802",
                            context=violation.context,
                        )
                    continue
                source = self._external_name(wrapper_class, stage, input_plan, value)
                streaming = value.streaming if isinstance(value, InputDeclaration) else input_plan.streaming
                input_modes[input_plan.name] = bool(streaming)
                streaming_declared = (
                    value.streaming_declared if isinstance(value, InputDeclaration) else input_plan.streaming_declared
                )
                existing = inputs.get(source)
                if existing is not None and existing.schema is not input_plan.schema:
                    raise self._error(
                        wrapper_class.__name__,
                        f"External input {source} is used with incompatible schemas.",
                        "Use distinct input names for distinct schemas.",
                    )
                aliases = input_plan.aliases
                if existing is not None:
                    aliases = self._aliases((*existing.aliases, *input_plan.aliases))
                inputs[source] = InputPlan(
                    name=source,
                    schema=input_plan.schema,
                    ordinal=0,
                    streaming=streaming,
                    aliases=aliases,
                    streaming_declared=streaming_declared,
                )
            effective_outputs[stage.name] = {
                output.name: output
                for output in self._effective_outputs(plan, input_modes)
            }
        return [replace(input, ordinal=ordinal) for ordinal, input in enumerate(inputs.values())], streaming_boundaries

    def _external_name(
        self,
        wrapper_class: type[Transform],
        stage: StageDeclaration,
        input_plan: InputPlan,
        value: object,
    ) -> str:
        if not isinstance(value, InputDeclaration):
            raise self._error(
                wrapper_class.__name__,
                f"{stage.name}.{input_plan.name} is bound to an unsupported value.",
                "Bind graph stage inputs to wrapper input(...) fields or earlier stage outputs.",
            )
        declared = wrapper_class._structure_inputs.get(value.name)
        if declared is not value:
            raise self._error(
                wrapper_class.__name__,
                f"{value.name or '<unnamed>'} is not an input on {wrapper_class.__name__}.",
                "Bind stage inputs only to input(...) fields declared on the wrapper transform.",
            )
        if value.schema is not input_plan.schema:
            raise self._error(
                wrapper_class.__name__,
                f"{value.name} declares {self._schema_name(value.schema)}, but {type(stage.invocation).__name__}.{input_plan.name} expects {self._schema_name(input_plan.schema)}.",
                "Bind only inputs with the same schema.",
            )
        return value.name

    def _rewrite(
        self,
        wrapper_class: type[Transform],
        stages: tuple[StageDeclaration, ...],
        stage_plans: tuple[TransformPlan, ...],
        *,
        stage_by_name: Mapping[str, StageDeclaration],
        rewrite_body: RewriteBody,
    ) -> tuple[list[StepPlan], list[OutputPlan], dict[tuple[StageDeclaration, str], OutputPlan]]:
        steps: list[StepPlan] = []
        stage_outputs: list[OutputPlan] = []
        output_sources: dict[tuple[StageDeclaration, str], OutputPlan] = {}

        for stage, plan in zip(stages, stage_plans, strict=True):
            label = stage.name or self._snake(type(stage.invocation).__name__)
            frame_map = self._stage_input_sources(
                wrapper_class, stage, plan, output_sources, stage_by_name=stage_by_name
            )
            for step in plan.steps:
                rewritten = self._step(step, label=label, frame_map=frame_map, rewrite_body=rewrite_body)
                rewritten = replace(rewritten, name=f"{label}.{step.name}", ordinal=len(steps))
                steps.append(rewritten)
                for original, result in zip(step.results, rewritten.results, strict=True):
                    frame_map[original.frame] = result.frame
                    frame_map[original.lane] = result.frame

            input_modes = {}
            for input_plan in plan.inputs:
                value = stage.invocation._structure_bound_inputs[input_plan.name]
                if isinstance(value, StageOutputReference):
                    upstream = output_sources.get((stage_by_name.get(value.stage.name, value.stage), value.name))
                    input_modes[input_plan.name] = bool(upstream and upstream.streaming)
                elif isinstance(value, InputDeclaration):
                    input_modes[input_plan.name] = bool(value.streaming)
                else:
                    input_modes[input_plan.name] = bool(input_plan.streaming)
            for output in self._effective_outputs(plan, input_modes):
                rewritten_output = replace(
                    output,
                    source=frame_map[output.source],
                    ordinal=len(stage_outputs),
                )
                output_sources[(stage, output.name)] = rewritten_output
                stage_outputs.append(rewritten_output)
        return steps, stage_outputs, output_sources

    def _effective_outputs(
        self,
        plan: TransformPlan,
        input_modes: Mapping[str, bool],
    ) -> tuple[OutputPlan, ...]:
        modes = {name: bool(streaming) for name, streaming in input_modes.items()}
        modes.update({f"input:{name}": bool(streaming) for name, streaming in input_modes.items()})
        for step in plan.steps:
            streaming = bool(modes.get(step.source)) or any(
                bool(modes.get(input.source)) for input in step.inputs
            )
            modes[step.name] = streaming
            modes[step.output_lane] = streaming
            for result in step.results:
                modes[result.lane] = streaming
                modes[result.frame] = streaming
        return tuple(replace(output, streaming=bool(modes.get(output.source, output.streaming))) for output in plan.outputs)

    def _stage_input_sources(
        self,
        wrapper_class: type[Transform],
        stage: StageDeclaration,
        plan: TransformPlan,
        output_sources: dict[tuple[StageDeclaration, str], OutputPlan],
        *,
        stage_by_name: Mapping[str, StageDeclaration],
    ) -> dict[str, str]:
        sources: dict[str, str] = {}
        bound = stage.invocation._structure_bound_inputs
        for input_plan in plan.inputs:
            value = bound[input_plan.name]
            if isinstance(value, StageOutputReference):
                upstream = output_sources.get((stage_by_name.get(value.stage.name, value.stage), value.name))
                if upstream is None:
                    raise self._error(
                        wrapper_class.__name__,
                        f"{stage.name}.{input_plan.name} references unavailable stage output {value.stage.name}.{value.name}.",
                        "Reference only outputs from earlier stages.",
                    )
                sources[input_plan.name] = upstream.source
                sources[f"input:{input_plan.name}"] = upstream.source
                continue
            if not isinstance(value, InputDeclaration):
                raise self._error(
                    wrapper_class.__name__,
                    f"{stage.name}.{input_plan.name} is bound to an unsupported value.",
                    "Bind graph stage inputs to wrapper input(...) fields or earlier stage outputs.",
                )
            source = value.name
            sources[input_plan.name] = source
            sources[f"input:{input_plan.name}"] = source
        return sources

    def _step(
        self,
        step: StepPlan,
        *,
        label: str,
        frame_map: dict[str, str],
        rewrite_body: RewriteBody,
    ) -> StepPlan:
        results = tuple(self._result(result, label=label) for result in step.results)
        primary = results[0]
        result_frames = {
            key: result.frame
            for original, result in zip(step.results, results, strict=True)
            for key in (original.lane, original.frame)
        }
        hook_frame_map = {**frame_map, **result_frames}
        return replace(
            step,
            source=frame_map.get(step.source, self._frame(label, step.source)),
            input_lane=frame_map.get(step.input_lane, self._frame(label, step.input_lane)),
            output_lane=primary.frame,
            before_hooks=tuple(self._hook(hook, label=label, frame_map=frame_map) for hook in step.before_hooks),
            after_hooks=tuple(self._hook(hook, label=label, frame_map=hook_frame_map) for hook in step.after_hooks),
            inputs=tuple(self._input(input, label=label, frame_map=frame_map) for input in step.inputs),
            results=results,
            plugin_body=None if step.plugin_body is None else rewrite_body(step.plugin_body, frame_map),
        )

    def _input(self, input: StepInputPlan, *, label: str, frame_map: dict[str, str]) -> StepInputPlan:
        return replace(
            input,
            source=frame_map.get(input.source, self._frame(label, input.source)),
            lane=frame_map.get(input.lane, self._frame(label, input.lane)),
        )

    def _result(self, result: StepResultPlan, *, label: str) -> StepResultPlan:
        frame = self._frame(label, result.frame)
        return replace(
            result,
            lane=frame,
            frame=frame,
            after_hooks=tuple(
                self._hook(hook, label=label, frame_map={result.lane: frame, result.frame: frame})
                for hook in result.after_hooks
            ),
        )

    def _hook(self, hook: HookPlan, *, label: str, frame_map: dict[str, str]) -> HookPlan:
        return replace(
            hook,
            target=f"{label}.{hook.target}",
            sources=tuple(frame_map.get(source, self._frame(label, source)) for source in hook.sources),
            outputs=tuple(self._hook_output(output, label=label, frame_map=frame_map) for output in hook.outputs),
        )

    def _hook_output(self, output, *, label: str, frame_map: dict[str, str]):
        return replace(output, name=frame_map.get(output.name, self._frame(label, output.name)))

    def _outputs(
        self,
        wrapper_class: type[Transform],
        stage_outputs: list[OutputPlan],
        output_sources: dict[tuple[StageDeclaration, str], OutputPlan],
        *,
        stage_by_name: Mapping[str, StageDeclaration],
    ) -> list[OutputPlan]:
        outputs: list[OutputPlan] = []
        for ordinal, declaration in enumerate(wrapper_class._structure_outputs.values()):
            source = self._declared_output(
                wrapper_class, declaration, stage_outputs, output_sources, stage_by_name=stage_by_name
            )
            outputs.append(
                OutputPlan(
                    name=declaration.name,
                    schema=declaration.schema,
                    source=source.source,
                    source_scope=source.source_scope,
                    source_schema=source.source_schema,
                    ordinal=ordinal,
                    aliases=declaration.aliases,
                    streaming=source.streaming,
                )
            )
        return outputs

    def _declared_output(
        self,
        wrapper_class: type[Transform],
        declaration: OutputDeclaration,
        stage_outputs: list[OutputPlan],
        output_sources: dict[tuple[StageDeclaration, str], OutputPlan],
        *,
        stage_by_name: Mapping[str, StageDeclaration],
    ) -> OutputPlan:
        has_block_binding = declaration.name in wrapper_class._structure_output_bindings
        source = wrapper_class._structure_output_bindings.get(declaration.name, declaration.source)
        if has_block_binding and not isinstance(source, StageOutputReference):
            raise self._error(
                wrapper_class.__name__,
                f"Output {declaration.name} is bound to an unsupported source.",
                "Use outputs = output(name=stage.output) with a declared stage output.",
            )
        if isinstance(source, StageOutputReference):
            output = output_sources.get((stage_by_name.get(source.stage.name, source.stage), source.name))
            if output is None:
                raise self._error(
                    wrapper_class.__name__,
                    f"Output {declaration.name} references unavailable stage output {source.stage.name}.{source.name}.",
                    "Bind wrapper outputs to existing stage outputs.",
                )
            if output.schema is not declaration.schema:
                raise self._error(
                    wrapper_class.__name__,
                    f"Output {declaration.name} declares {declaration.schema.__name__}, but bound stage output carries {output.schema.__name__}.",
                    "Bind wrapper outputs only to matching schemas.",
                )
            return output
        matches = [
            output
            for output in stage_outputs
            if output.name == declaration.name and output.schema is declaration.schema
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            names = ", ".join(output.source for output in matches)
            raise self._error(
                wrapper_class.__name__,
                f"Cannot infer output {declaration.name}; matched stage outputs: {names}.",
                "Use output(..., stage.output) to select the intended stage output.",
            )
        matches = [output for output in stage_outputs if output.schema is declaration.schema]
        if len(matches) == 1:
            return matches[0]
        names = ", ".join(output.name for output in matches) or "none"
        raise self._error(
            wrapper_class.__name__,
            f"Cannot infer output {declaration.name} for schema {declaration.schema.__name__}; matched stage outputs: {names}.",
            "Use a unique output name/schema pair or output(..., stage.output).",
        )

    def _validate_references(self, wrapper_class: type[Transform], stages: tuple[StageDeclaration, ...]) -> None:
        known: set[str] = set()
        declared = {stage.name for stage in stages}
        for stage in stages:
            for value in stage.invocation._structure_bound_inputs.values():
                if not isinstance(value, StageOutputReference):
                    continue
                if value.stage.name not in declared:
                    raise self._error(
                        wrapper_class.__name__,
                        f"{stage.name} references a stage that is not declared on {wrapper_class.__name__}.",
                        "Use only stage(...) fields declared on the same transform.",
                    )
                if value.stage.name not in known:
                    raise self._error(
                        wrapper_class.__name__,
                        f"{stage.name} references {value.stage.name}.{value.name} before it is available.",
                        "Declare dependency stages before stages that consume them.",
                    )
            known.add(stage.name)

    def _aliases(self, aliases: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(dict.fromkeys(aliases))

    def _snake(self, name: str) -> str:
        first = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", first).lower()

    def _frame(self, label: str, name: str) -> str:
        return f"{label}__{name.replace(':', '__')}"

    def _schema_name(self, schema: object) -> str:
        return getattr(schema, "__name__", repr(schema))

    def _error(
        self,
        source: str,
        problem: str,
        use: str,
        *,
        code: str = "DSL-E0402",
        context: dict[str, str] | None = None,
    ) -> StructureCompileError:
        return StructureCompileError(
            Diagnostic(
                entry=diagnostic_registry.get(code),
                problem=problem,
                use=use,
                context=context or {},
                source=source,
            )
        )
