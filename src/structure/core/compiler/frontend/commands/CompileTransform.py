from __future__ import annotations

import inspect
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import replace
from pathlib import Path
from typing import cast, get_args, get_origin, get_type_hints

from structure.core.compiler.diagnostics.api import Diagnostics, StructureCompileError
from structure.core.compiler.frontend.logic.CompilerInputCollector import CompilerInputCollector
from structure.core.compiler.frontend.logic.CompilerTransformMember import CompilerTransformMember
from structure.core.compiler.frontend.logic.CompilerTransformMemberCollector import CompilerTransformMemberCollector
from structure.core.compiler.frontend.logic.ComposeTransformPlans import ComposeTransformPlans
from structure.core.compiler.frontend.logic.GuardTransformStepCalls import GuardTransformStepCalls
from structure.core.compiler.frontend.logic.PatchParentStepCalls import ParentStepInvocation, PatchParentStepCalls
from structure.core.compiler.ir.model.HookPlan import HookPlan
from structure.core.compiler.ir.model.InputPlan import InputPlan
from structure.core.compiler.ir.model.OutputPlan import OutputPlan
from structure.core.compiler.ir.model.StepPlan import StepPlan
from structure.core.compiler.ir.model.StepResultPlan import StepResultPlan
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.BindingSelector import BindingSelector
from structure.core.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.core.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.core.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.core.dsl.model.transforms.SchemaMode import SchemaMode
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1 import (
    AuthoringAPI,
    StepAuthoringCapture,
    StepAuthoringInput,
    StepAuthoringRequest,
    StepAuthoringResult,
    StepInputPlan,
    TransformMemberOrigin,
)

SourceDeclaration = InputDeclaration | LaneDeclaration | BindingSelector
WriteDeclaration = LaneDeclaration | OutputDeclaration | BindingSelector
_diagnostic_project_root: ContextVar[Path | None] = ContextVar("diagnostic_project_root", default=None)
_authoring: ContextVar[tuple[object | None, str, Mapping[str, object], Mapping[str, object]]] = ContextVar(
    "structure_platform_authoring", default=(None, "", {}, {})
)


class CompileTransform:

    def __init__(self) -> None:
        self._composer = ComposeTransformPlans()
        self._diagnostic_source = Diagnostics().source()
        self._input_collector = CompilerInputCollector()
        self._member_collector = CompilerTransformMemberCollector()
        self._step_call_guards = GuardTransformStepCalls(error=self._error, is_step=self._compiled)
        self._parent_step_calls = PatchParentStepCalls()

    def __call__(
        self,
        transform_class: type[Transform] | TransformPipeline,
        *,
        config: StructureConfig | None = None,
        project_root: Path | str | None = None,
        overrides: Mapping[str, object] | None = None,
        **settings: object,
    ) -> TransformPlan:
        authoring = settings.pop("_authoring", None)
        target = str(settings.pop("_authoring_target", ""))
        plugin_configuration = settings.pop("_authoring_configuration", None)
        plugin_options = settings.pop("_authoring_plugin_options", None)
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
        token = _diagnostic_project_root.set(resolved.project_root)
        authoring_token = _authoring.set(
            (
                authoring,
                target,
                cast(Mapping[str, object], plugin_configuration or {}),
                cast(Mapping[str, object], plugin_options or {}),
            )
        )
        try:
            return self._compile(transform_class, config=resolved)
        finally:
            _authoring.reset(authoring_token)
            _diagnostic_project_root.reset(token)

    def _compile(
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
        self._require_module_level_schemas(transform_class)
        pipeline = getattr(transform_class, "_structure_pipeline", None)
        if pipeline is not None:
            return self._compose_pipeline(
                pipeline, name=transform_class.__name__, config=config, wrapper_class=transform_class
            )
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
        steps, lanes, explicit_outputs, diagnostics = self._steps(
            transform_class,
            inputs,
            capture_special_exprs="embed_exprs" in config.generated_code_options,
        )
        outputs = self._outputs(transform_class, lanes, explicit_outputs)
        return TransformPlan(
            name=transform_class.__name__,
            inputs=tuple(inputs),
            steps=tuple(steps),
            outputs=tuple(outputs),
            options=dict(transform_class.__dict__.get("_structure_transform_options", {})),
            diagnostics=tuple(diagnostics),
        )

    def _require_module_level_schemas(self, transform_class: type[Transform]) -> None:
        for owner in transform_class.__mro__:
            if owner is Transform:
                return
            if not isinstance(owner, type) or not issubclass(owner, Transform):
                continue
            for name, value in owner.__dict__.items():
                if self._nested_schema(value, owner):
                    nested = f"{owner.__name__}.{name}"
                    raise self._error(
                        "DSL-E0402",
                        transform_class=owner,
                        member=name,
                        problem=f"{nested} is a Schema declared inside a Transform.",
                        use=(
                            f"Move {name} to module scope, preferably to a model schema file, "
                            "or keep it beside this Transform when it is used only here."
                        ),
                    )

    @staticmethod
    def _nested_schema(value: object, owner: type[Transform]) -> bool:
        return (
            isinstance(value, type)
            and issubclass(value, Schema)
            and value is not Schema
            and value.__module__ == owner.__module__
            and value.__qualname__.startswith(f"{owner.__qualname__}.")
        )

    def _compose_pipeline(
        self,
        pipeline: TransformPipeline,
        *,
        name: str,
        config: StructureConfig,
        wrapper_class: type[Transform] | None = None,
    ) -> TransformPlan:
        authoring_api = cast(AuthoringAPI | None, _authoring.get()[0])
        if authoring_api is None:
            raise RuntimeError("Core authoring requires a selected platform authoring facet.")
        return self._composer(
            pipeline,
            name=name,
            compile_stage=lambda transform_class: self._compile(transform_class, config=config),
            rewrite_body=lambda body, frames: authoring_api.rewrite_body(body, frames=frames),
            wrapper_class=wrapper_class,
        )

    def _steps(
        self,
        transform_class: type[Transform],
        inputs: list[InputPlan],
        *,
        capture_special_exprs: bool,
    ) -> tuple[list[StepPlan], dict[str, dict[str, object]], set[str], list[Diagnostic]]:
        instance = transform_class()
        members = self._member_collector.collect(transform_class)
        steps: list[StepPlan] = []
        lanes: dict[str, dict[str, object]] = {}
        explicit_outputs: set[str] = set()
        diagnostics: list[Diagnostic] = []
        pending_raw: list[CompilerTransformMember] = []

        for member in members:
            if getattr(member.member, "_structure_raw", None) is not None:
                if steps:
                    self._attach_raw(transform_class, member, steps, lanes)
                else:
                    pending_raw.append(member)
                continue
            result = self._compile_step(
                transform_class,
                member,
                members,
                instance,
                steps,
                lanes,
                inputs,
                explicit_outputs,
                diagnostics,
                capture_special_exprs=capture_special_exprs,
            )
            if result is not None and pending_raw:
                for raw_member in pending_raw:
                    steps[-1] = self._attach_raw_before(transform_class, raw_member, steps[-1])
                pending_raw.clear()

        if pending_raw:
            names = ", ".join(member.name for member in pending_raw)
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

    def _attach_raw(
        self,
        transform_class: type[Transform],
        item: CompilerTransformMember,
        steps: list[StepPlan],
        lanes: dict[str, dict[str, object]],
    ) -> None:
        if not steps:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=item.name,
                problem=f"{transform_class.__name__}.{item.name} is @raw before any compiled step.",
                use="Place @raw after a step method, or select an input/lane explicitly in a later V3 revision.",
            )
        metadata = cast(dict[str, object], getattr(item.member, "_structure_raw"))
        target = steps[-1]
        inputs, outputs = self._raw_bindings(transform_class, target, metadata, member=item.name)
        hook_lanes, sources = self._raw_arguments(transform_class, inputs, outputs, lanes, member=item.name)
        output_lanes = tuple(result.lane for result in target.results) or (target.output_lane,)
        unknown = [declaration.name for declaration in outputs if declaration.name not in output_lanes]
        if unknown:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=item.name,
                problem=(f"@raw replaces lane(s) that {target.name} does not produce: {', '.join(unknown)}."),
                use=f"Select one of: {', '.join(output_lanes)}.",
            )
        target_backend, target_defaulted = cast(tuple[tuple[str, ...], bool], metadata["target_backend"])
        hook = HookPlan(
            name=item.name,
            phase="raw",
            target=target.name,
            lanes=hook_lanes,
            outputs=outputs,
            sources=sources,
            schema_mode=cast(SchemaMode, metadata["schema_mode"]),
            project_output=bool(metadata["project_output"]),
            streaming=bool(metadata["streaming"]),
            target_backend=target_backend,
            target_defaulted=target_defaulted,
            target_platform=cast(str | None, metadata["target_platform"]),
            origin=TransformMemberOrigin.of(item.owner, item.name),
        )
        self._attach_after_hook(steps, hook)

    def _attach_raw_before(
        self,
        transform_class: type[Transform],
        item: CompilerTransformMember,
        target: StepPlan,
    ) -> StepPlan:
        metadata = cast(dict[str, object], getattr(item.member, "_structure_raw"))
        inputs, outputs = self._raw_bindings(transform_class, target, metadata, member=item.name)
        available: dict[str, dict[str, object]] = {target.input_lane: {"source": target.source}}
        hook_lanes, sources = self._raw_arguments(transform_class, inputs, outputs, available, member=item.name)
        unknown = [declaration.name for declaration in outputs if declaration.name != target.input_lane]
        if unknown:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=item.name,
                problem=(
                    f"@raw before {target.name} replaces lane(s) that {target.name} does not consume: "
                    f"{', '.join(unknown)}."
                ),
                use=f"Select lane={target.input_lane}.",
            )
        target_backend, target_defaulted = cast(tuple[tuple[str, ...], bool], metadata["target_backend"])
        hook = HookPlan(
            name=item.name,
            phase="raw",
            target=target.name,
            lanes=hook_lanes,
            outputs=outputs,
            sources=sources,
            schema_mode=cast(SchemaMode, metadata["schema_mode"]),
            project_output=bool(metadata["project_output"]),
            streaming=bool(metadata["streaming"]),
            target_backend=target_backend,
            target_defaulted=target_defaulted,
            target_platform=cast(str | None, metadata["target_platform"]),
            origin=TransformMemberOrigin.of(item.owner, item.name),
        )
        return replace(target, before_hooks=(*target.before_hooks, hook))

    def _raw_bindings(self, transform_class, target: StepPlan, metadata: dict[str, object], *, member: str):
        inputs = metadata["inputs"]
        outputs = metadata["outputs"]
        if inputs is not None and outputs is not None:
            return cast(tuple, inputs), self._raw_outputs(transform_class, cast(tuple, outputs), member=member)
        if inputs is not None:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem="@raw(input=...) needs output=... or inout=... so returned DataFrames have destinations.",
                use="Use @raw(inout=sources | outputs).",
            )
        if outputs is not None:
            return (), self._raw_outputs(transform_class, cast(tuple, outputs), member=member)
        output_lanes = tuple(result.lane for result in target.results) or (target.output_lane,)
        if len(output_lanes) != 1:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"@raw after multi-output step {target.name} needs explicit lane(s)=... selection.",
                use=f"Use @raw(inout=source | target) with one or more of: {', '.join(output_lanes)}.",
            )
        declaration = self._raw_declaration(transform_class, output_lanes[0])
        return (declaration,), (declaration,)

    def _raw_outputs(self, transform_class, declarations: tuple, *, member: str) -> tuple:
        outputs = []
        for declaration in declarations:
            self._declared_write(transform_class, declaration, member=member)
            outputs.append(self._raw_declaration(transform_class, declaration.name))
        return tuple(outputs)

    def _raw_arguments(
        self,
        transform_class: type[Transform],
        inputs: tuple,
        outputs: tuple,
        available: dict[str, dict[str, object]],
        *,
        member: str,
    ) -> tuple[tuple, tuple[str, ...]]:
        arguments: list[object] = []
        sources: list[str] = []
        by_name: dict[str, str] = {}
        for declaration in inputs:
            argument, source = self._raw_input(transform_class, declaration, available, member=member)
            self._raw_argument(arguments, sources, by_name, argument, source, transform_class, member, output=False)
        for declaration in outputs:
            source = self._raw_output_source(transform_class, declaration, available, member=member)
            self._raw_argument(arguments, sources, by_name, declaration, source, transform_class, member, output=True)
        return tuple(arguments), tuple(sources)

    def _raw_input(self, transform_class, declaration, available, *, member: str) -> tuple[object, str]:
        if isinstance(declaration, BindingSelector):
            if declaration.role == "input":
                self._declared_selector(transform_class, declaration, member=member, role="input")
                return declaration.declaration, f"input:{declaration.name}"
            if declaration.role == "lane":
                self._declared_selector(transform_class, declaration, member=member, role="input")
                return declaration.declaration, self._raw_available(
                    declaration.name, available, transform_class, member
                )
        if isinstance(declaration, InputDeclaration):
            self._declared_input(transform_class, declaration, member=member)
            return declaration, str(available.get(declaration.name, {}).get("source", f"input:{declaration.name}"))
        if isinstance(declaration, OutputDeclaration):
            self._declared_output(transform_class, declaration, member=member, role="input")
            return declaration, self._raw_available(declaration.name, available, transform_class, member)
        self._declared_lane_declaration(transform_class, declaration, member=member, role="input")
        return declaration, self._raw_available(declaration.name, available, transform_class, member)

    def _raw_output_source(self, transform_class, declaration, available, *, member: str) -> str:
        self._declared_lane(transform_class, declaration, member=member, role="output")
        return self._raw_available(declaration.name, available, transform_class, member)

    def _raw_available(self, name, available, transform_class, member: str) -> str:
        source = available.get(name, {}).get("source")
        if source is not None:
            return str(source)
        raise self._error(
            "DSL-E0402",
            transform_class=transform_class,
            member=member,
            problem=f"@raw parameter {name} is not available at this source-order position.",
            use="Produce the selected lane or output in an earlier step, or select input(name) for an original input.",
            context={"parameter": name},
        )

    def _raw_argument(
        self, arguments, sources, by_name, declaration, source, transform_class, member, *, output: bool
    ) -> None:
        name = declaration.name
        previous = by_name.get(name)
        if previous is None:
            arguments.append(declaration)
            sources.append(source)
            by_name[name] = source
            return
        if previous != source and not output:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"@raw binds parameter {name} to conflicting frames.",
                use="Use distinct declarations or one explicit input(...), lane(...), or output(...) selector.",
                context={"parameter": name},
            )

    @staticmethod
    def _raw_declaration(transform_class: type[Transform], name: str):
        return (
            transform_class._structure_inputs.get(name)
            or transform_class._structure_lanes.get(name)
            or transform_class._structure_outputs.get(name)
        )

    @staticmethod
    def _attach_after_hook(steps: list[StepPlan], hook: HookPlan) -> None:
        target = steps[-1]
        if len(target.results) == 1:
            steps[-1] = replace(target, after_hooks=(*target.after_hooks, hook))
            return
        results = list(target.results)
        for index, result in enumerate(results):
            if result.lane == hook.outputs[0].name:
                results[index] = replace(result, after_hooks=(*result.after_hooks, hook))
                steps[-1] = replace(target, results=tuple(results))
                return

    def _compile_step(
        self,
        transform_class: type[Transform],
        item: CompilerTransformMember,
        members: tuple[CompilerTransformMember, ...],
        instance: Transform,
        steps: list[StepPlan],
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        explicit_outputs: set[str],
        diagnostics: list[Diagnostic],
        *,
        plan_name: str | None = None,
        capture_special_exprs: bool = False,
    ) -> tuple[StepResultPlan, ...] | None:
        name = item.name
        member = item.member
        hints = get_type_hints(member)
        output_schemas = self._return_schemas(hints.get("return"))
        if not output_schemas:
            if get_origin(hints.get("return")) is tuple:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=name,
                    problem=f"{transform_class.__name__}.{name} has an invalid tuple return annotation.",
                    use="Use a fixed tuple of Schema classes, such as tuple[Accepted, Audited].",
                )
            if item.overridden and any(
                self._return_schemas(get_type_hints(parent.member).get("return")) for parent in item.overridden
            ):
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=name,
                    problem=f"{transform_class.__name__}.{name} overrides an inherited step method but is not a step method.",
                    use="Keep the Schema return annotation or rename the helper method.",
                )
            return None
        metadata = getattr(member, "_structure_output_method", None)
        parameters = self._row_parameters(member, hints)
        bindings = self._input_bindings(
            transform_class,
            metadata,
            lanes,
            inputs,
            parameters,
            member=name,
        )
        output_lanes = self._output_lanes(
            transform_class,
            metadata,
            lanes,
            output_schemas,
            member=name,
            explicit_outputs=explicit_outputs,
            default_lane=bindings[0].lane,
        )
        options = self._step_options(item.owner, metadata)
        parent_call: dict[str, object] = {}
        authoring_body: object | None = None
        authoring, target, configuration, plugin_options = _authoring.get()
        authoring_api = cast(AuthoringAPI | None, authoring)
        if authoring_api is None:
            raise RuntimeError("Core authoring requires a selected platform authoring facet.")
        request = StepAuthoringRequest(
            target=target,
            configuration=configuration,
            name=plan_name or name,
            origin=TransformMemberOrigin.of(item.owner, name),
            inputs=tuple(
                StepAuthoringInput(
                    parameter=binding.parameter,
                    schema=binding.schema,
                    source=binding.source,
                    scope=binding.scope,
                    lane=binding.lane,
                    ordinal=binding.ordinal,
                    driving=binding.driving,
                )
                for binding in bindings
            ),
            results=tuple(
                StepAuthoringResult(schema=schema, lane=lane, frame=lane, ordinal=ordinal)
                for ordinal, (schema, lane) in enumerate(zip(output_schemas, output_lanes, strict=True))
            ),
            options=options,
            capture_special_exprs=capture_special_exprs,
            primary_span=self._diagnostic_source(
                transform_class,
                name,
                project_root=_diagnostic_project_root.get(),
            ),
            plugin_options=plugin_options,
        )
        authoring_session = authoring_api.open_step(request)
        arguments = authoring_session.arguments()
        if len(arguments) != len(bindings):
            raise self._error(
                "PLUGIN-E2708",
                transform_class=transform_class,
                member=name,
                problem=f"Plugin {target!r} supplied {len(arguments)} symbolic arguments for {len(bindings)} bindings.",
                use="Update the plugin authoring facet to return one argument per step input.",
            )
        try:
            with self._step_call_guards(transform_class, members, active=item):
                with self._parent_step_calls(
                    item,
                    invoke=lambda candidate: self._invoke_parent_step(
                        transform_class,
                        candidate,
                        members,
                        instance,
                        steps,
                        lanes,
                        inputs,
                        explicit_outputs,
                        diagnostics,
                        capture_special_exprs=capture_special_exprs,
                    ),
                    record_source=lambda source: parent_call.update(source=source),
                ):
                    with authoring_session:
                        result = member(instance, *arguments)
        except StructureCompileError:
            raise
        except Exception as error:
            raise self._error(
                "DSL-E0401",
                transform_class=transform_class,
                member=name,
                problem=f"{transform_class.__name__}.{name} uses unsupported symbolic code: {error}",
                use="Use Structure expression helpers, combine predicates with &, |, or ~, or move arbitrary PySpark to a hook.",
                context={"error": type(error).__name__},
            ) from error

        diagnostics.extend(cast(tuple[Diagnostic, ...], authoring_session.validate()))
        result_plans = [
            StepResultPlan(schema=schema, lane=lane, frame=lane, ordinal=ordinal, after_hooks=())
            for ordinal, (schema, lane) in enumerate(zip(output_schemas, output_lanes, strict=True))
        ]
        first = result_plans[0]
        bindings = self._parent_call_bindings(bindings, parent_call)
        driver = bindings[0]
        capture = authoring_session.capture(result)
        if not isinstance(capture, StepAuthoringCapture):
            raise TypeError("Plugin authoring capture must return StepAuthoringCapture")
        authoring_body = capture.body
        diagnostics.extend(cast(tuple[Diagnostic, ...], capture.diagnostics))
        steps.append(
            StepPlan(
                name=plan_name or name,
                input_schema=cast(type[Schema], driver.schema),
                output_schema=first.schema,
                source=driver.source,
                source_scope=driver.scope,
                input_lane=driver.lane,
                output_lane=first.lane,
                ordinal=len(steps),
                before_hooks=(),
                after_hooks=first.after_hooks if len(result_plans) == 1 else (),
                inputs=tuple(bindings),
                results=tuple(result_plans),
                options=options,
                origin=TransformMemberOrigin.of(item.owner, name),
                plugin_body=authoring_body,
            )
        )
        for result in result_plans:
            lanes[result.lane] = {
                "schema": result.schema,
                "source": result.frame,
                "scope": result.schema.__name__,
            }
        return tuple(result_plans)

    def _parent_call_bindings(
        self,
        bindings: list[StepInputPlan],
        parent_call: dict[str, object],
    ) -> list[StepInputPlan]:
        source = parent_call.get("source")
        if source is None:
            return bindings
        source = cast(dict[str, object], source)
        driver = bindings[0]
        return [
            StepInputPlan(
                parameter=driver.parameter,
                schema=cast(type[Schema], source["schema"]),
                source=str(source["source"]),
                scope=str(source["scope"]),
                lane=str(source["lane"]),
                ordinal=driver.ordinal,
                driving=True,
            ),
            *bindings[1:],
        ]

    def _invoke_parent_step(
        self,
        transform_class,
        candidate,
        members,
        instance,
        steps,
        lanes,
        inputs,
        explicit_outputs,
        diagnostics,
        *,
        capture_special_exprs: bool,
    ) -> ParentStepInvocation:
        result = self._compile_step(
            transform_class,
            candidate,
            members,
            instance,
            steps,
            lanes,
            inputs,
            explicit_outputs,
            diagnostics,
            plan_name=f"{candidate.owner.__name__}.{candidate.name}",
            capture_special_exprs=capture_special_exprs,
        )
        if result is None:
            raise TypeError(f"{candidate.source} is not a compiled step method")
        first = result[0]
        authoring_api = cast(AuthoringAPI | None, _authoring.get()[0])
        if authoring_api is None:
            raise RuntimeError("Core authoring requires a selected platform authoring facet.")
        values = authoring_api.result_arguments(
            tuple(
                StepAuthoringResult(schema=item.schema, lane=item.lane, frame=item.frame, ordinal=item.ordinal)
                for item in result
            )
        )
        return ParentStepInvocation(
            value=values[0] if len(values) == 1 else values,
            source={
                "lane": first.lane,
                "schema": first.schema,
                "source": first.frame,
                "scope": first.schema.__name__,
            },
        )

    def _compiled(self, member) -> bool:
        try:
            annotation = get_type_hints(member).get("return")
        except NameError:
            return False
        return bool(self._return_schemas(annotation))

    def _step_options(
        self,
        transform_class: type[Transform],
        metadata: dict[str, object] | None,
    ) -> dict[str, object] | None:
        options = dict(transform_class.__dict__.get("_structure_step_method_options", {}))
        if metadata:
            options.update(cast(dict[str, object], metadata.get("options", {})))
        return options or None

    def _input_bindings(
        self,
        transform_class: type[Transform],
        metadata: dict[str, object] | None,
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        parameters: tuple[inspect.Parameter, ...],
        *,
        member: str,
    ) -> list[StepInputPlan]:
        declarations = cast(tuple[SourceDeclaration, ...], metadata.get("inputs", ())) if metadata else ()
        if declarations and len(declarations) != len(parameters):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=(
                    f"@transform(input=...) binds {len(declarations)} source(s), "
                    f"but {transform_class.__name__}.{member} declares {len(parameters)} schema parameter(s)."
                ),
                use="List one declaration in input=[...] for every schema parameter, in order.",
            )
        if len(parameters) == 1:
            parameter = parameters[0]
            schema = cast(type[Schema], parameter.annotation)
            lane, source = self._driving_source(
                transform_class,
                declarations[0] if declarations else None,
                lanes,
                inputs,
                schema,
                member=member,
                parameter=parameter.name,
            )
            actual = cast(type[Schema], source["schema"])
            if schema is not actual:
                if lane == "df":
                    problem = (
                        f"{transform_class.__name__}.{member} expects {schema.__name__}, "
                        f"but the previous step method returns {actual.__name__}."
                    )
                elif source.get("kind") == "input":
                    problem = (
                        f"{transform_class.__name__}.{member} expects {schema.__name__}, "
                        f"but input {lane} declares {actual.__name__}."
                    )
                else:
                    problem = (
                        f"{transform_class.__name__}.{member} expects {schema.__name__}, "
                        f"but lane {lane} currently carries {actual.__name__}."
                    )
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=problem,
                    use="Reorder step methods or update the row parameter annotation to match the selected input lane.",
                    context={"expected": schema.__name__, "actual": actual.__name__},
                )
            return [
                StepInputPlan(
                    parameter=parameter.name,
                    schema=schema,
                    source=str(source["source"]),
                    scope=str(source["scope"]),
                    lane=lane,
                    ordinal=0,
                    driving=True,
                )
            ]

        bindings: list[StepInputPlan] = []
        used: set[tuple[str, str]] = set()
        for ordinal, parameter in enumerate(parameters):
            schema = cast(type[Schema], parameter.annotation)
            declaration = declarations[ordinal] if declarations else None
            lane, source = self._parameter_source(
                transform_class,
                declaration,
                lanes,
                inputs,
                schema,
                member=member,
                driving=ordinal == 0,
                parameter=parameter.name,
                used=used,
            )
            actual = cast(type[Schema], source["schema"])
            if schema is not actual:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=(
                        f"{transform_class.__name__}.{member}.{parameter.name} expects {schema.__name__}, "
                        f"but {lane} carries {actual.__name__}."
                    ),
                    use="Bind a declaration whose schema matches the parameter annotation.",
                )
            key = (lane, str(source["source"]))
            if key in used:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=f"{transform_class.__name__}.{member} binds {lane} more than once.",
                    use="Bind each schema parameter to a distinct input or available lane.",
                )
            used.add(key)
            bindings.append(
                StepInputPlan(
                    parameter=parameter.name,
                    schema=schema,
                    source=str(source["source"]),
                    scope=parameter.name,
                    lane=lane,
                    ordinal=ordinal,
                    driving=ordinal == 0,
                )
            )
        return bindings

    def _parameter_source(
        self,
        transform_class: type[Transform],
        declaration: SourceDeclaration | None,
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        schema: type[Schema],
        *,
        member: str,
        driving: bool,
        parameter: str,
        used: set[tuple[str, str]],
    ) -> tuple[str, dict[str, object]]:
        if declaration is not None:
            return self._declared_source(transform_class, declaration, lanes, inputs, schema, member=member)
        if driving:
            current = [
                (lane, source)
                for lane, source in lanes.items()
                if source["schema"] is schema and (lane, str(source["source"])) not in used
            ]
            preferred = self._preferred_source(current, parameter)
            if preferred is not None:
                return preferred
            if len(current) == 1:
                return current[0]
            if not current and lanes:
                lane, source = next(reversed(lanes.items()))
                actual = cast(type[Schema], source["schema"])
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=(
                        f"{transform_class.__name__}.{member} expects {schema.__name__}, "
                        f"but the previous step method returns {actual.__name__}."
                    ),
                    use="Add @transform(input=that_input) to restart from an original input, or update the row parameter annotation.",
                    context={"expected": schema.__name__, "actual": actual.__name__},
                )

        candidates: list[tuple[str, dict[str, object]]] = []
        for input_plan in inputs:
            source_name = f"input:{input_plan.name}" if input_plan.name in lanes else input_plan.name
            source = {
                "kind": "input",
                "schema": input_plan.schema,
                "source": source_name,
                "scope": input_plan.name,
            }
            if input_plan.schema is schema and (input_plan.name, source_name) not in used:
                candidates.append((input_plan.name, source))
        for lane, source in lanes.items():
            key = (lane, str(source["source"]))
            if source["schema"] is schema and key not in used:
                candidates.append((lane, source))
        preferred = self._preferred_source(candidates, parameter)
        if preferred is not None:
            return preferred
        if len(candidates) != 1:
            names = ", ".join(lane for lane, _ in candidates) or "none"
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"Cannot deduce parameter source for schema {schema.__name__}; matched sources: {names}.",
                use="Add @transform(input=[...]) with one declaration for every schema parameter, in method order.",
                context={"schema": schema.__name__, "matches": str(len(candidates))},
            )
        return candidates[0]

    def _driving_source(
        self,
        transform_class: type[Transform],
        declaration: SourceDeclaration | None,
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        input_schema: type[Schema],
        *,
        member: str,
        parameter: str,
    ) -> tuple[str, dict[str, object]]:
        if declaration is not None:
            return self._declared_source(transform_class, declaration, lanes, inputs, input_schema, member=member)
        return self._input_lane(transform_class, lanes, inputs, input_schema, member=member, parameter=parameter)

    def _declared_source(
        self,
        transform_class: type[Transform],
        declaration: SourceDeclaration,
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        schema: type[Schema],
        *,
        member: str,
    ) -> tuple[str, dict[str, object]]:
        if isinstance(declaration, BindingSelector):
            if declaration.role == "input":
                return self._selected_input_source(transform_class, declaration, member=member)
            if declaration.role == "lane":
                return self._selected_lane_source(transform_class, declaration, lanes, member=member)
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem="@transform(input=...) cannot select an output(...) role.",
                use="Use input(...) or lane(...) selectors for method input=.",
            )
        if isinstance(declaration, InputDeclaration):
            return self._declared_input_source(transform_class, declaration, lanes, inputs, schema, member=member)
        return self._declared_lane_source(transform_class, declaration, lanes, member=member)

    def _declared_input_source(
        self,
        transform_class: type[Transform],
        declaration: object,
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        schema: type[Schema],
        *,
        member: str,
    ) -> tuple[str, dict[str, object]]:
        if not isinstance(declaration, InputDeclaration):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem="@transform(input=...) source is not an input(...) field.",
                use="Use an input(...) field from the same transform class.",
            )
        self._declared_input(transform_class, declaration, member=member)
        lane_source = lanes.get(declaration.name)
        lane_matches = lane_source is not None and lane_source["schema"] is schema
        input_matches = declaration.schema is schema
        if lane_matches:
            assert lane_source is not None
            return declaration.name, lane_source
        if input_matches:
            source = f"input:{declaration.name}" if lane_source is not None else declaration.name
            return declaration.name, {
                "kind": "input",
                "schema": declaration.schema,
                "source": source,
                "scope": declaration.name,
            }
        if lane_source is not None:
            return declaration.name, lane_source
        return declaration.name, {
            "kind": "input",
            "schema": declaration.schema,
            "source": declaration.name,
            "scope": declaration.name,
        }

    def _selected_input_source(
        self,
        transform_class: type[Transform],
        selector: BindingSelector,
        *,
        member: str,
    ) -> tuple[str, dict[str, object]]:
        declaration = selector.declaration
        if not isinstance(declaration, InputDeclaration):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem="@transform(input=input(...)) must select an input(...) field.",
                use="Use input(that_input) to force the original runtime input.",
            )
        self._declared_input(transform_class, declaration, member=member)
        return declaration.name, {
            "kind": "input",
            "schema": declaration.schema,
            "source": f"input:{declaration.name}",
            "scope": declaration.name,
        }

    def _declared_lane_source(
        self,
        transform_class: type[Transform],
        declaration: object,
        lanes: dict[str, dict[str, object]],
        *,
        member: str,
    ) -> tuple[str, dict[str, object]]:
        if not isinstance(declaration, LaneDeclaration):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem="@transform(input=...) source is not a readable declaration.",
                use="Use input(...) or lane(...) declarations for method input=.",
            )
        self._declared_lane(transform_class, declaration, member=member, role="input")
        if declaration.name not in lanes:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"Lane {declaration.name} is not available yet.",
                use="Consume only lanes produced earlier in source order, or use input=that_input to start a funnel.",
                context={"lane": declaration.name},
            )
        return declaration.name, lanes[declaration.name]

    def _selected_lane_source(
        self,
        transform_class: type[Transform],
        selector: BindingSelector,
        lanes: dict[str, dict[str, object]],
        *,
        member: str,
    ) -> tuple[str, dict[str, object]]:
        self._declared_selector(transform_class, selector, member=member, role="input")
        if selector.name not in lanes:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"Lane {selector.name} is not available yet.",
                use="Consume only lanes produced earlier in source order, or use input(that_input) to start from raw input.",
                context={"lane": selector.name},
            )
        return selector.name, lanes[selector.name]

    def _output_lanes(
        self,
        transform_class: type[Transform],
        metadata: dict[str, object] | None,
        lanes: dict[str, dict[str, object]],
        output_schemas: tuple[type[Schema], ...],
        *,
        member: str,
        explicit_outputs: set[str],
        default_lane: str,
    ) -> tuple[str, ...]:
        declarations = cast(tuple[WriteDeclaration, ...], metadata.get("outputs", ())) if metadata else ()
        if declarations and len(declarations) != len(output_schemas):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=(
                    f"@transform(output=...) binds {len(declarations)} output(s), "
                    f"but {transform_class.__name__}.{member} returns {len(output_schemas)} schema value(s)."
                ),
                use="List one declaration in output=[...] for every returned schema, in order.",
            )
        if len(output_schemas) == 1:
            declaration = declarations[0] if declarations else None
            return (
                self._output_lane(
                    transform_class,
                    declaration,
                    output_schemas[0],
                    lanes=lanes,
                    member=member,
                    explicit_outputs=explicit_outputs,
                    default_lane=default_lane,
                ),
            )
        if declarations:
            output_lanes: list[str] = []
            for schema, declaration in zip(output_schemas, declarations, strict=True):
                self._declared_write(transform_class, declaration, member=member)
                if not self._write_compatible(schema, declaration):
                    raise self._error(
                        "DSL-E0402",
                        transform_class=transform_class,
                        member=member,
                        problem=(
                            f"Result {len(output_lanes)} returns {schema.__name__}, "
                            f"not {declaration.name}'s {declaration.schema.__name__}."
                        ),
                        use="Order output=[...] to match the tuple return annotation.",
                    )
                output_lanes.append(declaration.name)
                if self._writes_output(declaration) and declaration.name not in lanes:
                    explicit_outputs.add(declaration.name)
            return tuple(output_lanes)
        available = list(transform_class._structure_outputs.values())
        selected: list[str] = []
        claimed: set[str] = set()
        for ordinal, schema in enumerate(output_schemas):
            matches = [item for item in available if item.schema is schema and item.name not in claimed]
            if len(matches) != 1:
                names = ", ".join(item.name for item in matches) or "none"
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=(
                        f"Cannot deduce result {ordinal} for schema {schema.__name__}; " f"matched outputs: {names}."
                    ),
                    use="Add @transform(output=[...]) with one output declaration for every result, in return order.",
                )
            selected.append(matches[0].name)
            claimed.add(matches[0].name)
            explicit_outputs.add(matches[0].name)
        return tuple(selected)

    def _return_schemas(self, annotation: object) -> tuple[type[Schema], ...]:
        if self._is_schema(annotation):
            return (cast(type[Schema], annotation),)
        if get_origin(annotation) is not tuple:
            return ()
        arguments = get_args(annotation)
        if not arguments or len(arguments) == 2 and arguments[1] is Ellipsis:
            return ()
        if not all(self._is_schema(argument) for argument in arguments):
            return ()
        return cast(tuple[type[Schema], ...], arguments)

    def _input_lane(
        self,
        transform_class: type[Transform],
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        input_schema: type[Schema],
        *,
        member: str,
        parameter: str,
    ) -> tuple[str, dict[str, object]]:
        current = [(lane, source) for lane, source in lanes.items() if source["schema"] is input_schema]
        preferred = self._preferred_source(current, parameter)
        if preferred is not None:
            return preferred
        if len(current) == 1:
            return current[0]
        if len(current) > 1:
            names = ", ".join(lane for lane, _ in current)
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"Cannot deduce current lane for schema {input_schema.__name__}; matched lanes: {names}.",
                use="Add @transform(input=that_lane) to select the intended input lane.",
            )
        if lanes:
            lane, source = next(reversed(lanes.items()))
            actual = cast(type[Schema], source["schema"])
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=(
                    f"{transform_class.__name__}.{member} expects {input_schema.__name__}, "
                    f"but the previous step method returns {actual.__name__}."
                ),
                use="Add @transform(input=that_input) to select an original input or shadowing lane, or update the row parameter annotation.",
                context={"expected": input_schema.__name__, "actual": actual.__name__},
            )
        input_plan = self._input_for_schema(inputs, input_schema, parameter=parameter)
        return input_plan.name, {
            "kind": "input",
            "schema": input_plan.schema,
            "source": input_plan.name,
            "scope": input_plan.name,
        }

    def _output_lane(
        self,
        transform_class: type[Transform],
        declaration: WriteDeclaration | None,
        output_schema: type[Schema],
        *,
        lanes: dict[str, dict[str, object]],
        member: str,
        explicit_outputs: set[str],
        default_lane: str,
    ) -> str:
        if declaration is None:
            return default_lane
        self._declared_write(transform_class, declaration, member=member)
        if not self._write_compatible(output_schema, declaration):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=(
                    f"{transform_class.__name__}.{member} returns {output_schema.__name__}, "
                    f"not {declaration.schema.__name__}."
                ),
                use="Return the schema declared by the bound output(...) field.",
                context={"expected": declaration.schema.__name__, "actual": output_schema.__name__},
            )
        if self._writes_output(declaration) and declaration.name not in lanes:
            explicit_outputs.add(declaration.name)
        return declaration.name

    def _outputs(
        self,
        transform_class: type[Transform],
        lanes: dict[str, dict[str, object]],
        explicit_outputs: set[str],
    ) -> list[OutputPlan]:
        declarations = list(transform_class._structure_outputs.values())

        outputs: list[OutputPlan] = []
        for ordinal, declaration in enumerate(declarations):
            output_lanes = lanes
            if declaration.name not in explicit_outputs:
                _, source = self._implicit_output_lane(transform_class, declaration, lanes)
                output_lanes = {declaration.name: source}
            outputs.append(
                self._lane_output(
                    declaration.name,
                    declaration.schema,
                    output_lanes,
                    ordinal=ordinal,
                    transform_class=transform_class,
                    aliases=declaration.aliases,
                )
            )
        return outputs

    def _implicit_output_lane(
        self,
        transform_class: type[Transform],
        declaration: OutputDeclaration,
        lanes: dict[str, dict[str, object]],
    ) -> tuple[str, dict[str, object]]:
        matches = [(lane, source) for lane, source in lanes.items() if source["schema"] is declaration.schema]
        if len(matches) != 1:
            names = ", ".join(lane for lane, _ in matches) or "none"
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=(
                    f"Cannot deduce final output {declaration.name} for schema {declaration.schema.__name__}; "
                    f"matched lanes: {names}."
                ),
                use=f"Add @transform(output={declaration.name}) to the method that produces this output lane.",
                context={"output": declaration.name},
            )
        return matches[0]

    def _lane_output(
        self,
        name: str,
        schema: type[Schema],
        lanes: dict[str, dict[str, object]],
        *,
        ordinal: int,
        transform_class: type[Transform],
        aliases: tuple[str, ...] = (),
    ) -> OutputPlan:
        source = lanes.get(name)
        if source is None:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"Output lane {name} is not available.",
                use="Produce the lane earlier in source order before exposing it as a result.",
                context={"output": name},
            )
        actual_schema = cast(type[Schema], source["schema"])
        if actual_schema is not schema:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"Output {name} declares {schema.__name__}, but lane {name} carries {actual_schema.__name__}.",
                use="Update the final step method return annotation or the output contract schema.",
                context={"expected": schema.__name__, "actual": actual_schema.__name__},
            )
        return OutputPlan(
            name=name,
            schema=schema,
            source=str(source["source"]),
            source_scope=str(source["scope"]),
            source_schema=actual_schema,
            ordinal=ordinal,
            aliases=aliases,
        )

    def _declared_output(
        self,
        transform_class: type[Transform],
        declaration: OutputDeclaration,
        *,
        member: str,
        role: str,
    ) -> None:
        declared = transform_class._structure_outputs.get(declaration.name)
        if declared is not declaration:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"@transform({role}=...) references an output that is not declared on {transform_class.__name__}.",
                use="Use an output(...) field from the same transform class.",
                context={"output": declaration.name or "<unnamed>"},
            )

    def _declared_lane_declaration(
        self,
        transform_class: type[Transform],
        declaration: LaneDeclaration,
        *,
        member: str,
        role: str,
    ) -> None:
        declared = transform_class._structure_lanes.get(declaration.name)
        if declared is not declaration:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"{role}=... references a lane that is not declared on {transform_class.__name__}.",
                use="Use a lane(...) field from the same transform class.",
                context={"lane": declaration.name or "<unnamed>"},
            )

    def _declared_write(
        self,
        transform_class: type[Transform],
        declaration: WriteDeclaration,
        *,
        member: str,
    ) -> None:
        if isinstance(declaration, BindingSelector):
            self._declared_write_selector(transform_class, declaration, member=member)
            return
        if isinstance(declaration, LaneDeclaration):
            self._declared_lane_declaration(transform_class, declaration, member=member, role="output")
            return
        if isinstance(declaration, OutputDeclaration):
            self._declared_output(transform_class, declaration, member=member, role="output")
            return
        raise self._error(
            "DSL-E0402",
            transform_class=transform_class,
            member=member,
            problem="@transform(output=...) must reference a lane(...) or output(...) field.",
            use="Use output(s)=... to write declared intermediate lanes or final outputs.",
        )

    def _declared_write_selector(
        self,
        transform_class: type[Transform],
        selector: BindingSelector,
        *,
        member: str,
    ) -> None:
        if selector.role == "lane":
            self._declared_selector(transform_class, selector, member=member, role="output")
            return
        if selector.role == "output" and isinstance(selector.declaration, OutputDeclaration):
            self._declared_output(transform_class, selector.declaration, member=member, role="output")
            return
        raise self._error(
            "DSL-E0402",
            transform_class=transform_class,
            member=member,
            problem="@transform(output=...) must select lane(...) or output(...).",
            use="Use lane(that_lane) to write a working lane or output(that_output) to write a final result.",
        )

    def _write_compatible(self, schema: type[Schema], declaration: WriteDeclaration) -> bool:
        if isinstance(declaration, BindingSelector):
            if declaration.role == "lane":
                if isinstance(declaration.declaration, LaneDeclaration):
                    return issubclass(schema, declaration.schema)
                return True
            return declaration.role == "output" and schema is declaration.schema
        if isinstance(declaration, LaneDeclaration):
            return issubclass(schema, declaration.schema)
        return schema is declaration.schema

    def _writes_output(self, declaration: WriteDeclaration) -> bool:
        if isinstance(declaration, BindingSelector):
            return declaration.role == "output"
        return isinstance(declaration, OutputDeclaration)

    def _declared_input(
        self,
        transform_class: type[Transform],
        declaration: InputDeclaration,
        *,
        member: str,
    ) -> None:
        declared = transform_class._structure_inputs.get(declaration.name)
        if declared is not declaration:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"@transform(input=...) references an input that is not declared on {transform_class.__name__}.",
                use="Use an input(...) field from the same transform class.",
                context={"input": declaration.name or "<unnamed>"},
            )

    def _declared_lane(
        self,
        transform_class: type[Transform],
        declaration: InputDeclaration | LaneDeclaration | OutputDeclaration,
        *,
        member: str,
        role: str,
    ) -> None:
        if isinstance(declaration, InputDeclaration):
            self._declared_input(transform_class, declaration, member=member)
            return
        if isinstance(declaration, LaneDeclaration):
            self._declared_lane_declaration(transform_class, declaration, member=member, role=role)
            return
        if isinstance(declaration, OutputDeclaration):
            self._declared_output(transform_class, declaration, member=member, role=role)
            return
        raise self._error(
            "DSL-E0402",
            transform_class=transform_class,
            member=member,
            problem=f"@{role}(...) must reference an input(...), lane(...), or output(...) field.",
            use="Pass a class field declared as name = input(Schema), lane(Schema), or output(Schema).",
        )

    def _declared_selector(
        self,
        transform_class: type[Transform],
        selector: BindingSelector,
        *,
        member: str,
        role: str,
    ) -> None:
        declaration = selector.declaration
        if isinstance(declaration, InputDeclaration):
            self._declared_input(transform_class, declaration, member=member)
            return
        if isinstance(declaration, LaneDeclaration):
            self._declared_lane_declaration(transform_class, declaration, member=member, role=role)
            return
        if isinstance(declaration, OutputDeclaration):
            self._declared_output(transform_class, declaration, member=member, role=role)
            return

    def _row_parameters(self, method, hints: dict[str, object]) -> tuple[inspect.Parameter, ...]:
        parameters = list(inspect.signature(method).parameters.values())
        row_parameters = [parameter for parameter in parameters if parameter.name != "self"]
        if not row_parameters:
            raise self._error(
                "DSL-E0402",
                transform_class=None,
                member=method.__qualname__,
                problem=f"{method.__qualname__} must declare at least one schema parameter.",
                use="Declare a non-self parameter annotated with the driving input or previous output schema.",
            )

        resolved: list[inspect.Parameter] = []
        for parameter in row_parameters:
            annotation = hints.get(parameter.name)
            if not self._is_schema(annotation):
                raise self._error(
                    "DSL-E0402",
                    transform_class=None,
                    member=method.__qualname__,
                    problem=f"{method.__qualname__}.{parameter.name} must be annotated with a Schema.",
                    use="Annotate every step method parameter with a Schema class.",
                    context={"parameter": parameter.name},
                )
            resolved.append(parameter.replace(annotation=annotation))
        return tuple(resolved)

    def _input_for_schema(
        self,
        inputs: list[InputPlan],
        schema: type[Schema],
        *,
        parameter: str | None = None,
    ) -> InputPlan:
        matches = [input_plan for input_plan in inputs if input_plan.schema is schema]
        if parameter is not None:
            preferred = self._preferred_input(matches, parameter)
            if preferred is not None:
                return preferred
        if len(matches) != 1:
            names = ", ".join(input_plan.name for input_plan in matches) or "none"
            raise self._error(
                "DSL-E0402",
                transform_class=None,
                problem=f"Cannot deduce input for schema {schema.__name__}; matched inputs: {names}.",
                use="Add @transform(input=that_input) to the step method or declare exactly one matching input(...).",
                context={"schema": schema.__name__, "matches": str(len(matches))},
            )
        return matches[0]

    def _preferred_input(self, inputs: list[InputPlan], parameter: str) -> InputPlan | None:
        for name in self._source_name_choices(parameter):
            matches = [input_plan for input_plan in inputs if input_plan.name == name]
            if len(matches) == 1:
                return matches[0]
            if matches:
                return None
        return None

    def _preferred_source(
        self,
        candidates: list[tuple[str, dict[str, object]]],
        parameter: str,
    ) -> tuple[str, dict[str, object]] | None:
        for name in self._source_name_choices(parameter):
            matches = [candidate for candidate in candidates if candidate[0] == name]
            if len(matches) == 1:
                return matches[0]
            if matches:
                return None
        return None

    def _source_name_choices(self, parameter: str) -> tuple[str, ...]:
        end = len(parameter)
        while end and not parameter[end - 1].isalpha():
            end -= 1
        stem = parameter[:end]
        suffix = parameter[end:]
        if not stem:
            return (parameter,)
        plural = f"{stem}s{suffix}"
        return (parameter,) if plural == parameter else (parameter, plural)

    def _is_schema(self, value: object) -> bool:
        return isinstance(value, type) and issubclass(value, Schema)

    def _error(
        self,
        code: str,
        *,
        transform_class: type[Transform] | None,
        problem: str,
        use: str,
        member: str | None = None,
        context: dict[str, str] | None = None,
    ) -> StructureCompileError:
        source = member or ""
        if transform_class is not None:
            source = f"{transform_class.__module__}.{transform_class.__name__}"
            if member is not None:
                source = f"{source}.{member}"
        return StructureCompileError(
            Diagnostic(
                entry=diagnostic_registry.get(code),
                problem=problem,
                use=use,
                context=context or {},
                source=source,
                primary_span=self._diagnostic_source(
                    transform_class,
                    member,
                    project_root=_diagnostic_project_root.get(),
                ),
            )
        )
