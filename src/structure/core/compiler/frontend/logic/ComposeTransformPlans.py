from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import replace

from structure.core.compiler.diagnostics.api import StructureCompileError
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
from structure.plugin.api.v1 import InputPlan

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
    ) -> TransformPlan:
        rewrite = rewrite_body or (lambda body, _: body)
        stages = pipeline.stages
        if not stages:
            raise self._error(
                name, "Transform pipeline has no stages.", "Call Transform.to(...) with at least one stage."
            )
        stage_plans = tuple(compile_stage(stage.transform_class) for stage in stages)

        labels = self._labels(stages)
        inputs, external = self._inputs(name, stages, stage_plans, wrapper_class=wrapper_class)
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
            options={},
            diagnostics=tuple(diagnostic for plan in stage_plans for diagnostic in plan.diagnostics),
        )

    def _inputs(
        self,
        pipeline_name: str,
        stages: tuple[TransformPipelineStage, ...],
        stage_plans: tuple[TransformPlan, ...],
        *,
        wrapper_class: type[Transform] | None,
    ) -> tuple[list[InputPlan], dict[tuple[int, str], str]]:
        external: dict[tuple[int, str], str] = {}
        inputs: dict[str, InputPlan] = {}
        current_outputs: tuple[OutputPlan, ...] = ()

        for index, (stage, plan) in enumerate(zip(stages, stage_plans, strict=True)):
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
                        streaming=input_plan.streaming,
                        aliases=aliases,
                    )
                    external[(index, input_plan.name)] = source
                    continue
                if candidates:
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
            current_outputs = self._stage_outputs(pipeline_name, stage, plan)

        return [replace(input, ordinal=ordinal) for ordinal, input in enumerate(inputs.values())], external

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

            current_outputs = {}
            for output in self._stage_outputs(pipeline_name, stage, plan):
                rewritten_output = self._output(output, frame_map=frame_map, ordinal=len(final_outputs))
                current_outputs[output.name] = rewritten_output
                if final:
                    final_outputs.append(rewritten_output)

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
    ) -> tuple[OutputPlan, ...]:
        renames = getattr(stage.invocation, "_structure_output_renames", {})
        if not renames:
            return plan.outputs
        outputs = {output.name for output in plan.outputs}
        unknown = set(renames) - outputs
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
            for output in plan.outputs
        )

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

    def _error(self, source: str, problem: str, use: str) -> StructureCompileError:
        return StructureCompileError(
            Diagnostic(
                entry=diagnostic_registry.get("DSL-E0402"),
                problem=problem,
                use=use,
                context={},
                source=source,
            )
        )
