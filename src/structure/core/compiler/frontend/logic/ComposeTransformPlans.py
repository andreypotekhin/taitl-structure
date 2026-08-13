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
from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.core.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.core.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline, TransformPipelineStage
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1 import InputPlan, StreamingBoundaryPlan

CompileStage = Callable[[type[Transform]], TransformPlan]
RewriteBody = Callable[[object, Mapping[str, str]], object]


class ComposeTransformPlans:

    def __call__(
        self,
        pipeline: TransformPipeline,
        *,
        name: str,
        compile_stage: CompileStage,
        rewrite_body: RewriteBody | None = None,
        wrapper_class: type[Transform] | None = None,
        allow_stream_to_batch: bool = False,
        stream_to_batch_policy: str = "default",
    ) -> TransformPlan:
        rewrite = rewrite_body or (lambda body, _: body)
        stages = pipeline.stages
        if not stages:
            raise self._error(
                name, "Transform pipeline has no stages.", "Call invocation.to(...) with at least one stage."
            )
        stage_plans = tuple(compile_stage(stage.transform_class) for stage in stages)

        labels = self._labels(stages)
        inputs, internal_inputs, external, streaming_boundaries = self._inputs(
            name,
            stages,
            stage_plans,
            wrapper_class=wrapper_class,
            allow_stream_to_batch=allow_stream_to_batch,
            stream_to_batch_policy=stream_to_batch_policy,
        )
        steps, outputs = self._rewrite(
            name,
            stages,
            stage_plans,
            labels=labels,
            external=external,
            rewrite_body=rewrite,
        )
        return TransformPlan(
            name=name,
            inputs=tuple(inputs),
            steps=tuple(steps),
            outputs=tuple(outputs),
            internal_inputs=tuple(internal_inputs),
            options=Transform.resolve_transform_options(
                wrapper_class.__dict__.get("_structure_transform_options", {}) if wrapper_class is not None else {},
                inputs=inputs,
                transform_name=name,
            ),
            diagnostics=tuple(diagnostic for plan in stage_plans for diagnostic in plan.diagnostics),
            streaming_boundaries=tuple(
                boundary for plan in stage_plans for boundary in plan.streaming_boundaries
            ) + tuple(streaming_boundaries),
        )

    def _inputs(
        self,
        pipeline_name: str,
        stages: tuple[TransformPipelineStage, ...],
        stage_plans: tuple[TransformPlan, ...],
        *,
        wrapper_class: type[Transform] | None,
        allow_stream_to_batch: bool,
        stream_to_batch_policy: str,
    ) -> tuple[list[InputPlan], list[InputPlan], dict[tuple[int, str], str], list[StreamingBoundaryPlan]]:
        external: dict[tuple[int, str], str] = {}
        inputs: dict[str, InputPlan] = {}
        internal_inputs: dict[str, InputPlan] = {}
        streaming_boundaries: list[StreamingBoundaryPlan] = []
        current_outputs: tuple[OutputPlan, ...] = ()

        for index, (stage, plan) in enumerate(zip(stages, stage_plans, strict=True)):
            internal_inputs.update({input.name: input for input in plan.internal_inputs})
            bound = stage.invocation._structure_bound_inputs
            for input_plan in plan.inputs:
                candidates = self._matching_outputs(input_plan, current_outputs)
                explicit = input_plan.name in bound
                if explicit and candidates:
                    raise self._error(
                        pipeline_name,
                        f"{stage.transform_class.__name__}.{input_plan.name} is both explicitly bound and produced upstream.",
                        "Remove the constructor argument or split the pipeline so every input has one source.",
                    )
                if explicit:
                    source = self._external_name(
                        pipeline_name,
                        stage,
                        input_plan,
                        bound[input_plan.name],
                        wrapper_class=wrapper_class,
                    )
                    bound_value = bound[input_plan.name]
                    streaming = (
                        bound_value.streaming if isinstance(bound_value, InputDeclaration) else input_plan.streaming
                    )
                    streaming_declared = (
                        bound_value.streaming_declared
                        if isinstance(bound_value, InputDeclaration)
                        else input_plan.streaming_declared
                    )
                    optional = bound_value.optional if isinstance(bound_value, InputDeclaration) else input_plan.optional
                    existing = inputs.get(source)
                    if existing is not None and existing.schema is not input_plan.schema:
                        raise self._error(
                            pipeline_name,
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
                        optional=optional,
                    )
                    external[(index, input_plan.name)] = source
                    continue
                if candidates:
                    if len(candidates) == 1:
                        output = candidates[0]
                        consumer_options = plan.options or {}
                        local_allow = bool(consumer_options.get("allow_stream_to_batch"))
                        explicit_batch = input_plan.streaming_declared or consumer_options.get("streaming") is False
                        defer_boundary = (
                            output.streaming
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
                                    producer=self._output_label(output),
                                    output=output.name,
                                    consumer=self._snake(stage.transform_class.__name__),
                                    input=input_plan.name,
                                )
                            )
                        else:
                            violation = validate_streaming_input_binding(
                                producer=self._output_label(output),
                                output=output,
                                consumer=stage.transform_class.__name__,
                                input_plan=input_plan,
                                consumer_options=plan.options,
                                allow_stream_to_batch=allow_stream_to_batch,
                            )
                        if violation is not None:
                            raise self._error(
                                pipeline_name,
                                violation.problem,
                                violation.use,
                                code="STREAM-E0802",
                                context=violation.context,
                            )
                    continue
                if input_plan.optional:
                    source = self._internal_name(index, input_plan.name)
                    internal_inputs[source] = InputPlan(
                        name=source,
                        schema=input_plan.schema,
                        ordinal=0,
                        streaming=input_plan.streaming,
                        aliases=input_plan.aliases,
                        streaming_declared=input_plan.streaming_declared,
                        optional=True,
                    )
                    external[(index, input_plan.name)] = source
                    continue
                raise self._error(
                    pipeline_name,
                    f"{stage.transform_class.__name__}.{input_plan.name} is not supplied.",
                    "Pass it in the transform constructor or pipe from an upstream transform with a matching output.",
                )
            if index and not any(input_plan.name not in bound for input_plan in plan.inputs):
                raise self._error(
                    pipeline_name,
                    f"{stage.transform_class.__name__} does not consume an upstream output.",
                    "Each .to(...) stage must consume at least one output from the incoming transform.",
                )
            effective_inputs = {
                input_plan.name: self._input_streaming(
                    index,
                    input_plan,
                    external=external,
                    inputs=inputs,
                    current_outputs=current_outputs,
                )
                for input_plan in plan.inputs
            }
            current_outputs = self._stage_outputs(
                pipeline_name,
                stage,
                plan,
                outputs=self._effective_outputs(plan, effective_inputs),
            )

        return (
            [replace(input, ordinal=ordinal) for ordinal, input in enumerate(inputs.values())],
            [replace(input, ordinal=ordinal) for ordinal, input in enumerate(internal_inputs.values())],
            external,
            streaming_boundaries,
        )

    def _internal_name(self, index: int, input_name: str) -> str:
        return f"__optional_stage_{index}_{input_name}"

    def _matching_outputs(self, input_plan: InputPlan, outputs: tuple[OutputPlan, ...]) -> tuple[OutputPlan, ...]:
        matches = [output for output in outputs if output.schema is input_plan.schema]
        for name in (*input_plan.aliases, input_plan.name):
            aliased = [output for output in matches if name in output.aliases]
            if len(aliased) == 1:
                return (aliased[0],)
            if aliased:
                return tuple(aliased)
            named = [output for output in matches if output.name == name]
            if len(named) == 1:
                return (named[0],)
            if named:
                return tuple(named)
        if len(matches) == 1:
            return (matches[0],)
        if matches:
            names = ", ".join(self._output_label(output) for output in matches)
            raise self._error(
                input_plan.name,
                f"Cannot choose an upstream output for {input_plan.name}; matched outputs: {names}.",
                "Bind the input explicitly in the transform constructor or add a unique output alias.",
            )
        return ()

    def _external_name(
        self,
        pipeline_name: str,
        stage: TransformPipelineStage,
        input_plan: InputPlan,
        value: object,
        *,
        wrapper_class: type[Transform] | None,
    ) -> str:
        if isinstance(value, LaneDeclaration):
            raise self._error(
                pipeline_name,
                f"{stage.transform_class.__name__}.{input_plan.name} is bound to a lane.",
                "Composition can bind only constructor inputs, declared transform inputs, and declared transform outputs.",
            )
        if isinstance(value, OutputDeclaration):
            raise self._error(
                pipeline_name,
                f"{stage.transform_class.__name__}.{input_plan.name} is bound to an output declaration.",
                "Use output declarations only as upstream composition results, not constructor arguments.",
            )
        if isinstance(value, InputDeclaration):
            if wrapper_class is None:
                raise self._error(
                    pipeline_name,
                    f"{stage.transform_class.__name__}.{input_plan.name} is bound to an input declaration outside a transform class.",
                    "Use declaration bindings only in a generated-capable wrapper transform class.",
                )
            declared = wrapper_class._structure_inputs.get(value.name)
            if declared is not value:
                raise self._error(
                    pipeline_name,
                    f"{value.name or '<unnamed>'} is not an input on {wrapper_class.__name__}.",
                    "Bind stage constructor inputs to input(...) fields declared on the wrapper transform.",
                )
            if value.schema is not input_plan.schema:
                raise self._error(
                    pipeline_name,
                    f"{value.name} declares {value.schema.__name__}, but {stage.transform_class.__name__}.{input_plan.name} expects {getattr(input_plan.schema, '__name__', input_plan.schema)}.",
                    "Bind only inputs with the same schema.",
                )
            return value.name
        return input_plan.name

    def _rewrite(
        self,
        pipeline_name: str,
        stages: tuple[TransformPipelineStage, ...],
        stage_plans: tuple[TransformPlan, ...],
        *,
        labels: tuple[str, ...],
        external: dict[tuple[int, str], str],
        rewrite_body: RewriteBody,
    ) -> tuple[list[StepPlan], list[OutputPlan]]:
        steps: list[StepPlan] = []
        current_outputs: dict[str, OutputPlan] = {}
        final_outputs: list[OutputPlan] = []

        for index, (stage, label, plan) in enumerate(zip(stages, labels, stage_plans, strict=True)):
            final = index == len(stage_plans) - 1
            input_sources = self._stage_input_sources(pipeline_name, index, plan, external, current_outputs)
            frame_map = dict(input_sources)
            rewritten_steps: list[StepPlan] = []
            final_names = {output.name for output in plan.outputs} if final else set()

            for step in plan.steps:
                rewritten = self._step(
                    step,
                    label=label,
                    frame_map=frame_map,
                    final_names=final_names,
                    rewrite_body=rewrite_body,
                )
                rewritten = replace(rewritten, name=f"{label}.{step.name}", ordinal=len(steps))
                rewritten_steps.append(rewritten)
                steps.append(rewritten)
                for original, result in zip(step.results, rewritten.results, strict=True):
                    frame_map[original.frame] = result.frame
                    frame_map[original.lane] = result.frame

            stage_outputs = self._stage_outputs(
                pipeline_name,
                stage,
                plan,
                outputs=self._effective_outputs(
                    plan,
                    {
                        input_plan.name: self._input_streaming_from_sources(
                            input_plan, input_sources, current_outputs
                        )
                        for input_plan in plan.inputs
                    },
                ),
            )
            next_outputs: dict[str, OutputPlan] = {}
            for output in stage_outputs:
                rewritten_output = self._output(output, frame_map=frame_map, ordinal=len(final_outputs))
                next_outputs[output.name] = rewritten_output
                if final:
                    final_outputs.append(rewritten_output)
            current_outputs = next_outputs

        if not final_outputs:
            raise self._error(
                pipeline_name, "Transform pipeline has no outputs.", "Use a final stage with output(...)."
            )
        return steps, final_outputs

    def _stage_input_sources(
        self,
        pipeline_name: str,
        index: int,
        plan: TransformPlan,
        external: dict[tuple[int, str], str],
        current_outputs: dict[str, OutputPlan],
    ) -> dict[str, str]:
        sources: dict[str, str] = {}
        for internal in plan.internal_inputs:
            sources[internal.name] = internal.name
            sources[f"input:{internal.name}"] = internal.name
        for input_plan in plan.inputs:
            external_name = external.get((index, input_plan.name))
            if external_name is not None:
                sources[input_plan.name] = external_name
                sources[f"input:{input_plan.name}"] = external_name
                continue
            matches = self._matching_outputs(input_plan, tuple(current_outputs.values()))
            if len(matches) != 1:
                raise self._error(
                    pipeline_name,
                    f"{input_plan.name} does not have one upstream source.",
                    "Pass the input in the constructor or make the upstream output match unambiguous.",
                )
            sources[input_plan.name] = matches[0].source
            sources[f"input:{input_plan.name}"] = matches[0].source
        return sources

    def _step(
        self,
        step: StepPlan,
        *,
        label: str,
        frame_map: dict[str, str],
        final_names: set[str],
        rewrite_body: RewriteBody,
    ) -> StepPlan:
        results = tuple(self._result(result, label=label, final_names=final_names) for result in step.results)
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

    def _result(self, result: StepResultPlan, *, label: str, final_names: set[str]) -> StepResultPlan:
        frame = result.frame if result.frame in final_names else self._frame(label, result.frame)
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

    def _output(self, output: OutputPlan, *, frame_map: dict[str, str], ordinal: int) -> OutputPlan:
        return replace(
            output,
            source=frame_map[output.source],
            ordinal=ordinal,
        )

    def _stage_outputs(
        self,
        pipeline_name: str,
        stage: TransformPipelineStage,
        plan: TransformPlan,
        *,
        outputs: tuple[OutputPlan, ...] | None = None,
    ) -> tuple[OutputPlan, ...]:
        outputs_to_rewrite = plan.outputs if outputs is None else outputs
        renames = getattr(stage.invocation, "_structure_output_renames", {})
        if not renames:
            return outputs_to_rewrite
        output_names = {output.name for output in outputs_to_rewrite}
        unknown = set(renames) - output_names
        if unknown:
            raise self._error(
                pipeline_name,
                f"{stage.transform_class.__name__}.rename(...) references unknown output(s): {', '.join(sorted(unknown))}.",
                "Rename outputs declared by that transform stage.",
            )
        return tuple(
            (
                replace(output, aliases=self._aliases((*output.aliases, renames[output.name])))
                if output.name in renames
                else output
            )
            for output in outputs_to_rewrite
        )

    def _input_streaming(
        self,
        index: int,
        input_plan: InputPlan,
        *,
        external: dict[tuple[int, str], str],
        inputs: dict[str, InputPlan],
        current_outputs: tuple[OutputPlan, ...],
    ) -> bool:
        external_name = external.get((index, input_plan.name))
        if external_name is not None:
            return bool(inputs[external_name].streaming)
        matches = self._matching_outputs(input_plan, current_outputs)
        return bool(matches[0].streaming) if len(matches) == 1 else bool(input_plan.streaming)

    def _input_streaming_from_sources(
        self,
        input_plan: InputPlan,
        input_sources: dict[str, str],
        current_outputs: dict[str, OutputPlan],
    ) -> bool:
        source = input_sources.get(input_plan.name)
        if source is None:
            return bool(input_plan.streaming)
        return any(output.source == source and output.streaming for output in current_outputs.values()) or bool(
            input_plan.streaming
        )

    def _effective_outputs(
        self,
        plan: TransformPlan,
        input_modes: dict[str, bool],
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

    def _aliases(self, aliases: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(dict.fromkeys(aliases))

    def _output_label(self, output: OutputPlan) -> str:
        if not output.aliases:
            return output.name
        return f"{output.name} alias {', '.join(output.aliases)}"

    def _labels(self, stages: tuple[TransformPipelineStage, ...]) -> tuple[str, ...]:
        counts: dict[str, int] = {}
        labels: list[str] = []
        for stage in stages:
            base = self._snake(stage.transform_class.__name__)
            counts[base] = counts.get(base, 0) + 1
            labels.append(base if counts[base] == 1 else f"{base}_{counts[base]}")
        return tuple(labels)

    def _snake(self, name: str) -> str:
        first = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", first).lower()

    def _frame(self, label: str, name: str) -> str:
        return f"{label}__{name.replace(':', '__')}"

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
