from __future__ import annotations

import inspect
from contextlib import contextmanager
from typing import cast, get_args, get_origin, get_type_hints

from structure.app.compiler.diagnostics.api import StructureCompileError
from structure.app.compiler.frontend.logic.CompilerHookCollector import CompilerHookCollector
from structure.app.compiler.frontend.logic.CompilerInputCollector import CompilerInputCollector
from structure.app.compiler.frontend.logic.CompilerTransformMember import CompilerTransformMember
from structure.app.compiler.frontend.logic.CompilerTransformMemberCollector import CompilerTransformMemberCollector
from structure.app.compiler.frontend.logic.ComposeTransformPlans import ComposeTransformPlans
from structure.app.compiler.ir.model.AggregateAssignment import AggregateAssignment
from structure.app.compiler.ir.model.AggregateKey import AggregateKey
from structure.app.compiler.ir.model.AggregatePlan import AggregatePlan
from structure.app.compiler.ir.model.HookPlan import HookPlan
from structure.app.compiler.ir.model.InputPlan import InputPlan
from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.compiler.ir.model.OutputPlan import OutputPlan
from structure.app.compiler.ir.model.ProjectAssignment import ProjectAssignment
from structure.app.compiler.ir.model.StepInputPlan import StepInputPlan
from structure.app.compiler.ir.model.StepPlan import StepPlan
from structure.app.compiler.ir.model.StepResultPlan import StepResultPlan
from structure.app.compiler.ir.model.TransformMemberOrigin import TransformMemberOrigin
from structure.app.compiler.ir.model.TransformPlan import TransformPlan
from structure.app.compiler.symbolic_execution.model.CompileContext import CompileContext
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.expr.InputScope import InputScope
from structure.app.dsl.model.expr.RowScope import RowScope
from structure.app.dsl.model.schemas.Projection import Projection
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.AsOf import AsOf
from structure.app.dsl.model.transforms.BindingSelector import BindingSelector
from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.Join import Join
from structure.app.dsl.model.transforms.JoinDedupe import JoinDedupe
from structure.app.dsl.model.transforms.JoinHint import JoinHint
from structure.app.dsl.model.transforms.JoinStrategy import JoinStrategy
from structure.app.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.model.transforms.OverlapPolicy import OverlapPolicy
from structure.app.dsl.model.transforms.reserved_v2 import reserved_operations
from structure.app.dsl.model.transforms.TiePolicy import TiePolicy
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.dsl.model.transforms.TransformPipeline import TransformPipeline
from structure.app.dsl.model.types.BooleanType import BooleanType
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.lib.cross.errors import Diagnostic, diagnostic_registry

SourceDeclaration = InputDeclaration | LaneDeclaration | BindingSelector
WriteDeclaration = LaneDeclaration | OutputDeclaration | BindingSelector


class CompileTransform:

    def __init__(self) -> None:
        self._composer = ComposeTransformPlans()
        self._hook_collector = CompilerHookCollector()
        self._input_collector = CompilerInputCollector()
        self._member_collector = CompilerTransformMemberCollector()

    def __call__(self, transform_class: type[Transform] | TransformPipeline) -> TransformPlan:
        if isinstance(transform_class, TransformPipeline):
            return self._compose_pipeline(transform_class, name="ComposedTransform")
        if not isinstance(transform_class, type) or not issubclass(transform_class, Transform) or transform_class is Transform:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class if isinstance(transform_class, type) else None,
                problem=f"{getattr(transform_class, '__name__', transform_class)} is not a Transform subclass.",
                use="Compile a class that inherits from structure.Transform or compile a Transform.to(...) pipeline.",
            )
        pipeline = getattr(transform_class, "_structure_pipeline", None)
        if pipeline is not None:
            return self._compose_pipeline(pipeline, name=transform_class.__name__, wrapper_class=transform_class)
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
        steps, lanes, explicit_outputs, diagnostics = self._steps(transform_class, inputs)
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
        wrapper_class: type[Transform] | None = None,
    ) -> TransformPlan:
        return self._composer(
            pipeline,
            name=name,
            compile_stage=self.__call__,
            wrapper_class=wrapper_class,
        )

    def _steps(
        self,
        transform_class: type[Transform],
        inputs: list[InputPlan],
    ) -> tuple[list[StepPlan], dict[str, dict[str, object]], set[str], list[Diagnostic]]:
        instance = transform_class()
        members = self._member_collector.collect(transform_class)
        hooks = self._hook_collector.collect(transform_class, members)
        steps: list[StepPlan] = []
        lanes: dict[str, dict[str, object]] = {}
        explicit_outputs: set[str] = set()
        diagnostics: list[Diagnostic] = []

        for member in members:
            self._compile_step(
                transform_class,
                member,
                members,
                instance,
                hooks,
                steps,
                lanes,
                inputs,
                explicit_outputs,
                diagnostics,
            )

        if not steps:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"{transform_class.__name__} has no public schema-returning subtransform.",
                use="Add a public instance method with a Structure row parameter and Structure return annotation.",
            )
        return steps, lanes, explicit_outputs, diagnostics

    def _compile_step(
        self,
        transform_class: type[Transform],
        item: CompilerTransformMember,
        members: tuple[CompilerTransformMember, ...],
        instance: Transform,
        hooks: dict[tuple[str, tuple[type[Transform], str, int]], tuple[HookPlan, ...]],
        steps: list[StepPlan],
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        explicit_outputs: set[str],
        diagnostics: list[Diagnostic],
        *,
        plan_name: str | None = None,
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
                    use="Use a fixed tuple of Structure schemas, such as tuple[Accepted, Audited].",
                )
            if item.overridden and any(self._return_schemas(get_type_hints(parent.member).get("return")) for parent in item.overridden):
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=name,
                    problem=f"{transform_class.__name__}.{name} overrides an inherited subtransform but is not a subtransform.",
                    use="Keep the Structure return annotation or rename the helper method.",
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

        context = CompileContext(step=plan_name or name)
        arguments = [
            (
                RowScope(name=binding.scope, schema=binding.schema)
                if binding.driving
                else InputScope(name=binding.scope, schema=binding.schema, source=binding.source)
            )
            for binding in bindings
        ]
        context.default_project_source = arguments[0]
        context.register_current_scope(bindings[0].scope)
        for binding, argument in zip(bindings[1:], arguments[1:], strict=True):
            context.register_relation_scope(binding.scope, argument)

        try:
            with self._subtransform_call_guards(transform_class, members, active=item):
                with self._parent_call_patches(
                    transform_class,
                    item,
                    members,
                    instance,
                    hooks,
                    steps,
                    lanes,
                    inputs,
                    explicit_outputs,
                    diagnostics,
                    parent_call,
                ):
                    with context:
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

        context.operations.extend(self._reserved_operations(member, metadata))
        diagnostics.extend(self._validate_joins(transform_class, name, context.joins))
        values = self._result_values(
            transform_class,
            name,
            output_schemas,
            result,
        )
        if context.aggregate_keys is not None and len(values) > 1:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=name,
                problem=f"{transform_class.__name__}.{name} uses group_by(...) with multiple returned schemas.",
                use="Return one aggregate schema per grouped subtransform.",
            )
        result_plans: list[StepResultPlan] = []
        after_hooks = hooks.get(("after", item.key), ())
        for hook in after_hooks:
            for lane in hook.lanes:
                self._declared_lane(transform_class, lane, member=hook.name, role="lane")
            for output in hook.outputs:
                self._declared_lane(transform_class, output, member=hook.name, role="output")
            unknown = [output.name for output in hook.outputs if output.name not in output_lanes]
            if unknown:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=hook.name,
                    problem=f"@after({name}) replaces lane(s) that {name} does not produce: {', '.join(unknown)}.",
                    use=f"Select one of: {', '.join(output_lanes)}.",
                )
        for ordinal, (output_schema, output_lane, value) in enumerate(
            zip(output_schemas, output_lanes, values, strict=True)
        ):
            selected_hooks = self._result_hooks(
                transform_class,
                name,
                output_lane,
                after_hooks,
                multiple=len(output_schemas) > 1,
            )
            frame = output_lane
            aggregate = (
                None
                if context.aggregate_keys is None
                else self._aggregate_plan(
                    transform_class,
                    name,
                    output_schema,
                    value,
                    keys=context.aggregate_keys,
                    grouping=context.aggregate_grouping,
                    filters=context.filters,
                )
            )
            result_plans.append(
                StepResultPlan(
                    schema=output_schema,
                    lane=output_lane,
                    frame=frame,
                    projection=tuple(
                        ()
                        if aggregate is not None
                        else self._assignments(
                            transform_class,
                            name,
                            output_schema,
                            value,
                            filters=context.filters,
                        )
                    ),
                    ordinal=ordinal,
                    aggregate=aggregate,
                    after_hooks=selected_hooks,
                )
            )
        first = result_plans[0]
        if first.aggregate is not None:
            context.operations.append(OperationPlan.aggregate_operation(first.aggregate))
        bindings = self._parent_call_bindings(bindings, parent_call)
        driver = bindings[0]
        before_hooks = self._before_hooks(
            transform_class,
            name,
            driver.lane,
            hooks.get(("before", item.key), ()),
        )
        self._validate_relation_reads(
            transform_class,
            name,
            bindings,
            context.operations,
            result_plans,
        )
        steps.append(
            StepPlan(
                name=plan_name or name,
                input_schema=driver.schema,
                output_schema=first.schema,
                source=driver.source,
                source_scope=driver.scope,
                input_lane=driver.lane,
                output_lane=first.lane,
                filters=tuple(context.filters),
                projection=first.projection,
                ordinal=len(steps),
                aggregate=first.aggregate,
                joins=tuple(context.joins),
                operations=tuple(context.operations),
                before_hooks=before_hooks,
                after_hooks=first.after_hooks if len(result_plans) == 1 else (),
                inputs=tuple(bindings),
                results=tuple(result_plans),
                options=options,
                origin=TransformMemberOrigin.of(item.owner, name),
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
                schema=cast(type[Structure], source["schema"]),
                source=str(source["source"]),
                scope=str(source["scope"]),
                lane=str(source["lane"]),
                ordinal=driver.ordinal,
                driving=True,
            ),
            *bindings[1:],
        ]

    @contextmanager
    def _parent_call_patches(
        self,
        transform_class: type[Transform],
        item: CompilerTransformMember,
        members: tuple[CompilerTransformMember, ...],
        instance: Transform,
        hooks: dict[tuple[str, tuple[type[Transform], str, int]], tuple[HookPlan, ...]],
        steps: list[StepPlan],
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        explicit_outputs: set[str],
        diagnostics: list[Diagnostic],
        parent_call: dict[str, object],
    ):
        originals: list[tuple[type[Transform], str, object]] = []
        scheduled: dict[CompilerTransformMember, tuple[StepResultPlan, ...]] = {}

        def stub(candidate: CompilerTransformMember):
            def call(_self, *args, **kwargs):
                if kwargs:
                    raise TypeError("Parent subtransform calls must use positional schema arguments")
                result = scheduled.get(candidate)
                if result is None:
                    result = self._compile_step(
                        transform_class,
                        candidate,
                        members,
                        instance,
                        hooks,
                        steps,
                        lanes,
                        inputs,
                        explicit_outputs,
                        diagnostics,
                        plan_name=f"{candidate.owner.__name__}.{candidate.name}",
                    )
                    if result is None:
                        raise TypeError(f"{candidate.source} is not a compiled subtransform")
                    scheduled[candidate] = result
                value = self._parent_call_result(result)
                first = result[0]
                parent_call["source"] = {
                    "lane": first.lane,
                    "schema": first.schema,
                    "source": first.frame,
                    "scope": first.schema.__name__,
                }
                return value

            return call

        try:
            for candidate in item.overridden:
                originals.append((candidate.owner, candidate.name, candidate.owner.__dict__[candidate.name]))
                setattr(candidate.owner, candidate.name, stub(candidate))
            yield
        finally:
            for owner, name, original in reversed(originals):
                setattr(owner, name, original)

    @contextmanager
    def _subtransform_call_guards(
        self,
        transform_class: type[Transform],
        members: tuple[CompilerTransformMember, ...],
        *,
        active: CompilerTransformMember,
    ):
        originals: list[tuple[type[Transform], str, object]] = []
        guarded: set[tuple[type[Transform], str]] = set()

        def guard(owner: type[Transform], name: str):
            def call(_self, *args, **kwargs):
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=active.name,
                    problem=(
                        f"{transform_class.__name__}.{active.name} calls compiled subtransform "
                        f"{owner.__name__}.{name} directly."
                    ),
                    use=(
                        "Subtransforms are pipeline steps. Use source order, lane bindings, Transform.to(...), "
                        "a private helper, or an @expr_fn helper instead. Only an override may call its overridden "
                        "parent subtransform."
                    ),
                    context={"called_subtransform": f"{owner.__name__}.{name}"},
                )

            return call

        try:
            for candidate in self._guarded_subtransforms(transform_class, members):
                key = (candidate.owner, candidate.name)
                if key in guarded:
                    continue
                originals.append((candidate.owner, candidate.name, candidate.owner.__dict__[candidate.name]))
                setattr(candidate.owner, candidate.name, guard(candidate.owner, candidate.name))
                guarded.add(key)
            yield
        finally:
            for owner, name, original in reversed(originals):
                setattr(owner, name, original)

    def _guarded_subtransforms(
        self,
        transform_class: type[Transform],
        members: tuple[CompilerTransformMember, ...],
    ) -> tuple[CompilerTransformMember, ...]:
        guarded: list[CompilerTransformMember] = []
        seen: set[tuple[type[Transform], str]] = set()

        def add(candidate: CompilerTransformMember) -> None:
            key = (candidate.owner, candidate.name)
            if key not in seen and self._compiled(candidate.member):
                guarded.append(candidate)
                seen.add(key)

        for member in members:
            add(member)
            for candidate in member.overridden:
                add(candidate)

        for cls in transform_class.__mro__:
            if cls is Transform:
                break
            if not isinstance(cls, type) or not issubclass(cls, Transform):
                continue
            for name, member in cls.__dict__.items():
                if name.startswith("_") or name == "run" or not inspect.isfunction(member):
                    continue
                add(CompilerTransformMember(owner=cls, name=name, member=member, position=0))
        for cls in self._loaded_transform_classes():
            for name, member in cls.__dict__.items():
                if name.startswith("_") or name == "run" or not inspect.isfunction(member):
                    continue
                add(CompilerTransformMember(owner=cls, name=name, member=member, position=0))

        return tuple(guarded)

    def _loaded_transform_classes(self) -> tuple[type[Transform], ...]:
        classes: list[type[Transform]] = []

        def visit(cls: type[Transform]) -> None:
            for subclass in cls.__subclasses__():
                classes.append(subclass)
                visit(subclass)

        visit(Transform)
        return tuple(classes)

    def _compiled(self, member) -> bool:
        return bool(self._return_schemas(get_type_hints(member).get("return")))

    def _parent_call_result(self, results: tuple[StepResultPlan, ...]) -> RowScope | tuple[RowScope, ...]:
        scopes = tuple(RowScope(name=result.schema.__name__, schema=result.schema) for result in results)
        if len(scopes) == 1:
            return scopes[0]
        return scopes

    def _step_options(
        self,
        transform_class: type[Transform],
        metadata: dict[str, object] | None,
    ) -> dict[str, object] | None:
        options = dict(transform_class.__dict__.get("_structure_subtransform_options", {}))
        if metadata:
            options.update(cast(dict[str, object], metadata.get("options", {})))
        return options or None

    def _reserved_operations(self, member, metadata: dict[str, object] | None) -> tuple[OperationPlan, ...]:
        operations = tuple(reserved_operations(member))
        if metadata:
            operations += cast(tuple[OperationPlan, ...], metadata.get("reserved_operations", ()))
        return operations

    def _validate_relation_reads(
        self,
        transform_class: type[Transform],
        member: str,
        bindings: list[StepInputPlan],
        operations: list,
        results: list[StepResultPlan],
    ) -> None:
        joined: set[str] = set()
        relation_scopes = {binding.scope: binding.parameter for binding in bindings[1:]}
        for operation in operations:
            if operation.kind == "filter" and operation.filter is not None:
                self._validate_joined_relation_reads(
                    transform_class,
                    member,
                    relation_scopes,
                    joined,
                    self._scopes(operation.filter),
                )
            if operation.kind == "join" and operation.join is not None:
                if operation.join.temporal is not None:
                    self._validate_joined_relation_reads(
                        transform_class,
                        member,
                        relation_scopes,
                        joined,
                        self._scopes(operation.join.temporal.at),
                    )
                if operation.join.as_of is not None:
                    self._validate_joined_relation_reads(
                        transform_class,
                        member,
                        relation_scopes,
                        joined,
                        self._scopes(operation.join.as_of.left_time),
                    )
                    if operation.join.as_of.tolerance is not None:
                        self._validate_joined_relation_reads(
                            transform_class,
                            member,
                            relation_scopes,
                            joined,
                            self._scopes(operation.join.as_of.tolerance),
                        )
                if operation.join.method.exposes_fields():
                    joined.add(operation.join.input_name)
            if operation.kind == "aggregate" and operation.aggregate is not None:
                reads = set().union(*(self._scopes(key.expression) for key in operation.aggregate.keys))
                reads.update(
                    *(
                        self._scopes(assignment.expression)
                        for assignment in operation.aggregate.assignments
                        if assignment.expression is not None
                    )
                )
                self._validate_joined_relation_reads(transform_class, member, relation_scopes, joined, reads)
            if operation.kind == "selected_rows" and operation.selected_rows is not None:
                reads = set().union(
                    self._scopes(operation.selected_rows.order_by),
                    *(self._scopes(expression) for expression in operation.selected_rows.partition_by),
                )
                self._validate_joined_relation_reads(transform_class, member, relation_scopes, joined, reads)
            if operation.kind == "drop_duplicates" and operation.duplicate_rows is not None:
                reads = set().union(*(self._scopes(expression) for expression in operation.duplicate_rows.subset))
                self._validate_joined_relation_reads(transform_class, member, relation_scopes, joined, reads)

        reads = set().union(
            *(self._scopes(assignment.expression) for result in results for assignment in result.projection)
        )
        self._validate_joined_relation_reads(transform_class, member, relation_scopes, joined, reads)

    def _validate_joined_relation_reads(
        self,
        transform_class: type[Transform],
        member: str,
        relation_scopes: dict[str, str],
        joined: set[str],
        reads: set[str],
    ) -> None:
        for scope, parameter in relation_scopes.items():
            if scope in reads and scope not in joined:
                raise self._error(
                    "JOIN-E0601",
                    transform_class=transform_class,
                    member=member,
                    problem=(
                        f"{transform_class.__name__}.{member} reads relation parameter "
                        f"{parameter} before it is joined."
                    ),
                    use=(f"Use left_join({parameter}, on=...) or lookup_join({parameter}, on=...) before reading its fields."),
                    context={"input": parameter},
                )

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
            schema = cast(type[Structure], parameter.annotation)
            lane, source = self._driving_source(
                transform_class,
                declarations[0] if declarations else None,
                lanes,
                inputs,
                schema,
                member=member,
                parameter=parameter.name,
            )
            actual = cast(type[Structure], source["schema"])
            if schema is not actual:
                if lane == "df":
                    problem = (
                        f"{transform_class.__name__}.{member} expects {schema.__name__}, "
                        f"but the previous subtransform returns {actual.__name__}."
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
                    use="Reorder subtransforms or update the row parameter annotation to match the selected input lane.",
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
            schema = cast(type[Structure], parameter.annotation)
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
            actual = cast(type[Structure], source["schema"])
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
        schema: type[Structure],
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
                actual = cast(type[Structure], source["schema"])
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=(
                        f"{transform_class.__name__}.{member} expects {schema.__name__}, "
                        f"but the previous subtransform returns {actual.__name__}."
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
        input_schema: type[Structure],
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
        schema: type[Structure],
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
        schema: type[Structure],
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
        output_schemas: tuple[type[Structure], ...],
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

    def _return_schemas(self, annotation: object) -> tuple[type[Structure], ...]:
        if self._is_schema(annotation):
            return (cast(type[Structure], annotation),)
        if get_origin(annotation) is not tuple:
            return ()
        arguments = get_args(annotation)
        if not arguments or len(arguments) == 2 and arguments[1] is Ellipsis:
            return ()
        if not all(self._is_schema(argument) for argument in arguments):
            return ()
        return cast(tuple[type[Structure], ...], arguments)

    def _result_values(
        self,
        transform_class: type[Transform],
        member: str,
        schemas: tuple[type[Structure], ...],
        result: object,
    ) -> tuple[Structure | Projection, ...]:
        if len(schemas) == 1:
            return (cast(Structure | Projection, result),)
        if not isinstance(result, tuple) or len(result) != len(schemas):
            actual = len(result) if isinstance(result, tuple) else type(result).__name__
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=(
                    f"{transform_class.__name__}.{member} must return {len(schemas)} schema values; got {actual}."
                ),
                use="Return a tuple whose values match the fixed tuple annotation in order.",
            )
        if any(isinstance(value, Projection) for value in result):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"{transform_class.__name__}.{member} uses project(...) in a multi-output return.",
                use="Return explicit schema instances for tuple-returning subtransforms.",
            )
        return cast(tuple[Structure | Projection, ...], result)

    def _result_hooks(
        self,
        transform_class: type[Transform],
        member: str,
        lane: str,
        hooks: tuple[HookPlan, ...],
        *,
        multiple: bool,
    ) -> tuple[HookPlan, ...]:
        selected: list[HookPlan] = []
        for hook in hooks:
            if hook.outputs[0].name == lane:
                self._validate_hook_signature(transform_class, hook)
                selected.append(hook)
        return tuple(selected)

    def _before_hooks(
        self,
        transform_class: type[Transform],
        member: str,
        lane: str,
        hooks: tuple[HookPlan, ...],
    ) -> tuple[HookPlan, ...]:
        for hook in hooks:
            for source in hook.lanes:
                self._declared_lane(transform_class, source, member=hook.name, role="lane")
            for output in hook.outputs:
                self._declared_lane(transform_class, output, member=hook.name, role="output")
            unknown = [output.name for output in hook.outputs if output.name != lane]
            if unknown:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=hook.name,
                    problem=f"@before({member}) replaces lane(s) that {member} does not consume: {', '.join(unknown)}.",
                    use=f"Select lane={lane}.",
                )
            self._validate_hook_signature(transform_class, hook)
        return hooks

    def _input_lane(
        self,
        transform_class: type[Transform],
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        input_schema: type[Structure],
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
            actual = cast(type[Structure], source["schema"])
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=(
                    f"{transform_class.__name__}.{member} expects {input_schema.__name__}, "
                    f"but the previous subtransform returns {actual.__name__}."
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
        output_schema: type[Structure],
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
        schema: type[Structure],
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
        actual_schema = cast(type[Structure], source["schema"])
        if actual_schema is not schema:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"Output {name} declares {schema.__name__}, but lane {name} carries {actual_schema.__name__}.",
                use="Update the final subtransform return annotation or the output contract schema.",
                context={"expected": schema.__name__, "actual": actual_schema.__name__},
            )
        return OutputPlan(
            name=name,
            schema=schema,
            source=str(source["source"]),
            source_scope=str(source["scope"]),
            source_schema=actual_schema,
            filters=(),
            projection=(),
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

    def _write_compatible(self, schema: type[Structure], declaration: WriteDeclaration) -> bool:
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

    def _validate_hook_signature(self, transform_class: type[Transform], hook: HookPlan) -> None:
        self._validate_hook_target_backend(transform_class, hook)
        method = getattr(transform_class, hook.name)
        parameters = list(inspect.signature(method).parameters.values())
        if not parameters or parameters[0].name != "self":
            raise self._hook_signature_error(
                transform_class,
                hook,
                problem=f"{transform_class.__name__}.{hook.name} must declare self.",
            )
        runtime = parameters[1:]
        if any(parameter.kind is not inspect.Parameter.KEYWORD_ONLY for parameter in runtime):
            raise self._hook_signature_error(
                transform_class,
                hook,
                problem=f"{transform_class.__name__}.{hook.name} hook parameters must be keyword-only.",
            )
        expected = [lane.name for lane in hook.lanes] + ["spark", "ctx"]
        if hook.pass_inputs:
            expected.insert(len(hook.lanes), "inputs")
        names = [parameter.name for parameter in runtime]
        if names != expected:
            raise self._hook_signature_error(
                transform_class,
                hook,
                problem=(
                    f"{transform_class.__name__}.{hook.name} must declare keyword-only parameters "
                    f"{', '.join(expected)}; got {', '.join(names) or 'none'}."
                ),
            )

    def _validate_hook_target_backend(self, transform_class: type[Transform], hook: HookPlan) -> None:
        if "all" in hook.target_backend or "pyspark" in hook.target_backend:
            return
        targets = ", ".join(hook.target_backend)
        raise self._error(
            "DSL-E0402",
            transform_class=transform_class,
            member=hook.name,
            problem=(
                f"{transform_class.__name__}.{hook.name} targets {targets}, "
                "but v1 active hook execution is PySpark only."
            ),
            use='Use target_backend="pyspark" for v1, or keep non-PySpark hook declarations for a future backend.',
            context={"hook": hook.name, "target_backend": targets},
        )

    def _hook_signature_error(
        self,
        transform_class: type[Transform],
        hook: HookPlan,
        *,
        problem: str,
    ) -> StructureCompileError:
        inputs = ", inputs" if hook.pass_inputs else ""
        lane_names = ", ".join(lane.name for lane in hook.lanes)
        return self._error(
            "DSL-E0402",
            transform_class=transform_class,
            member=hook.name,
            problem=problem,
            use=f"Use def {hook.name}(self, *, {lane_names}{inputs}, spark, ctx): ...",
            context={"hook": hook.name, "lane": lane_names},
        )

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
                    problem=f"{method.__qualname__}.{parameter.name} must be annotated with a Structure schema.",
                    use="Annotate every subtransform parameter with a Structure schema class.",
                    context={"parameter": parameter.name},
                )
            resolved.append(parameter.replace(annotation=annotation))
        return tuple(resolved)

    def _input_for_schema(
        self,
        inputs: list[InputPlan],
        schema: type[Structure],
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
                use="Add @transform(input=that_input) to the subtransform or declare exactly one matching input(...).",
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

    def _assignments(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Structure],
        result: Structure | Projection,
        *,
        filters: tuple[Expression, ...] | list[Expression],
    ) -> list[ProjectAssignment]:
        if isinstance(result, Projection):
            return self._projection_assignments(
                transform_class,
                member,
                output_schema,
                result,
                filters=filters,
            )
        if not isinstance(result, output_schema):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"Subtransform returned {type(result).__name__}, not {output_schema.__name__}.",
                use="Return an instance of the schema declared in the subtransform return annotation.",
                context={"expected": output_schema.__name__, "actual": type(result).__name__},
            )

        assignments: list[ProjectAssignment] = []
        for field in output_schema._structure_fields.values():
            if field.name not in result._structure_values:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=f"{output_schema.__name__}.{field.name} is not assigned.",
                    use="Assign every declared output field, or return an inherited base schema with explicit overrides.",
                    context={"field": field.name, "schema": output_schema.__name__},
                )
            expression = literal(result._structure_values[field.name])
            assignments.append(
                self._assignment(transform_class, member, output_schema, field, expression, filters=filters)
            )
        return assignments

    def _aggregate_plan(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Structure],
        result: Structure | Projection,
        *,
        keys: tuple[tuple[str, Expression], ...],
        grouping: str,
        filters: tuple[Expression, ...] | list[Expression],
    ) -> AggregatePlan:
        if isinstance(result, Projection) or not isinstance(result, output_schema):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"{transform_class.__name__}.{member} uses group_by(...) but does not return {output_schema.__name__}.",
                use="Return an aggregate output schema instance with grouped keys and aggregate expressions.",
            )

        aggregate_keys = tuple(AggregateKey(name=name, expression=expression) for name, expression in keys)
        assignments: list[AggregateAssignment] = []
        for field in output_schema._structure_fields.values():
            if field.name not in result._structure_values:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=f"{output_schema.__name__}.{field.name} is not assigned.",
                    use="Assign every aggregate output field.",
                    context={"field": field.name, "schema": output_schema.__name__},
                )
            expression = literal(result._structure_values[field.name])
            key = self._aggregate_key_for(field.name, expression, aggregate_keys)
            if key is not None:
                self._assignment(transform_class, member, output_schema, field, expression, filters=filters)
                assignments.append(
                    AggregateAssignment(field=field, function="key", expression=expression, key=key.name)
                )
                continue
            if expression.kind == "aggregate":
                self._validate_aggregate_expression(transform_class, member, output_schema, field.name, expression)
                self._assignment(
                    transform_class,
                    member,
                    output_schema,
                    field,
                    expression,
                    filters=(),
                    allow_aggregate=True,
                )
                data = expression.data or {}
                function = str(data.get("function"))
                arg_count = self._int_data(data, "arg_count", len(expression.args))
                arguments = expression.args[:arg_count]
                where_index = self._optional_int_data(data, "where_index")
                order_by_index = self._optional_int_data(data, "order_by_index")
                metric_filter = expression.args[where_index] if where_index is not None else None
                order_by = expression.args[order_by_index] if order_by_index is not None else None
                options = tuple(
                    (key, value)
                    for key, value in data.items()
                    if key
                    not in {
                        "function",
                        "capability_group",
                        "capability_name",
                        "arg_count",
                        "where_index",
                        "order_by_index",
                    }
                )
                assignments.append(
                    AggregateAssignment(
                        field=field,
                        function=function,
                        expression=arguments[0] if arguments else None,
                        arguments=arguments,
                        filter=metric_filter,
                        order_by=order_by,
                        options=options,
                    )
                )
                continue
            if self._can_first(expression, aggregate_keys):
                self._assignment(transform_class, member, output_schema, field, expression, filters=filters)
                assignments.append(AggregateAssignment(field=field, function="first", expression=expression))
                continue
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"{output_schema.__name__}.{field.name} is neither a grouped key nor an aggregate expression.",
                use="Assign a group_by(...) key, count(), sum(...), or a grouped parent field.",
                context={"field": field.name, "schema": output_schema.__name__},
            )
        return AggregatePlan(keys=aggregate_keys, assignments=tuple(assignments), grouping=grouping)

    def _validate_aggregate_expression(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Structure],
        field: str,
        expression: Expression,
    ) -> None:
        data = expression.data or {}
        function = str(data.get("function"))
        arg_count = self._int_data(data, "arg_count", len(expression.args))
        arguments = expression.args[:arg_count]
        argument = arguments[0] if arguments else None
        if function in {"count", "grouping_id"}:
            return
        if function == "is_grouped" and argument is not None:
            return
        if argument is None:
            raise self._aggregate_error(transform_class, member, output_schema, field, function, "an input expression")
        numeric_functions = {
            "avg",
            "sum",
            "stddev",
            "variance",
            "corr",
            "covar",
            "approx_percentile",
        }
        if function in numeric_functions and not all(self._numeric_type(item.type) for item in arguments):
            raise self._aggregate_error(transform_class, member, output_schema, field, function, "a numeric expression")
        if function in {"max", "min", "first_value", "last_value"} and not self._orderable_type(argument.type):
            raise self._aggregate_error(transform_class, member, output_schema, field, function, "an orderable scalar expression")
        if function in {"count_distinct", "approx_count_distinct"} and not self._scalar_type(argument.type):
            raise self._aggregate_error(transform_class, member, output_schema, field, function, "a scalar expression")
        if function in {"bool_and", "bool_or"} and not isinstance(argument.type, BooleanType):
            raise self._aggregate_error(transform_class, member, output_schema, field, function, "a Boolean expression")

    def _aggregate_error(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Structure],
        field: str,
        function: str,
        expected: str,
    ) -> StructureCompileError:
        return self._error(
            "DSL-E0402",
            transform_class=transform_class,
            member=member,
            problem=f"{output_schema.__name__}.{field} uses {function}(...) with unsupported input type.",
            use=f"Pass {expected} to {function}(...), or move custom aggregation logic into an explicit hook.",
            context={"field": field, "schema": output_schema.__name__, "function": function},
        )

    def _int_data(self, data, key: str, default: int) -> int:
        value = data.get(key, default)
        return value if isinstance(value, int) else default

    def _optional_int_data(self, data, key: str) -> int | None:
        value = data.get(key)
        return value if isinstance(value, int) else None

    def _aggregate_key_for(
        self,
        field: str,
        expression: Expression,
        keys: tuple[AggregateKey, ...],
    ) -> AggregateKey | None:
        for key in keys:
            if key.name == field or self._same_expression(key.expression, expression):
                return key
        return None

    def _can_first(self, expression: Expression, keys: tuple[AggregateKey, ...]) -> bool:
        return any(self._field_contains(expression, key.expression) for key in keys)

    def _same_expression(self, left: Expression, right: Expression) -> bool:
        return left.kind == right.kind and left.data == right.data and left.args == right.args

    def _field_contains(self, parent: Expression, child: Expression) -> bool:
        if parent.kind != "field" or child.kind != "field" or not parent.data or not child.data:
            return False
        if parent.data.get("scope") != child.data.get("scope"):
            return False
        parent_field = str(parent.data.get("field"))
        child_field = str(child.data.get("field"))
        return child_field.startswith(f"{parent_field}.")

    def _projection_assignments(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Structure],
        result: Projection,
        *,
        filters: tuple[Expression, ...] | list[Expression],
    ) -> list[ProjectAssignment]:
        if result.target is not None and result.target is not output_schema:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=(
                    f"{transform_class.__name__}.{member} returns {output_schema.__name__}, "
                    f"but project(...) targets {result.target.__name__}."
                ),
                use="Make the project(...) target match the subtransform return annotation.",
                context={"expected": output_schema.__name__, "actual": result.target.__name__},
            )
        source_schema = self._source_schema(result.source)
        if source_schema is None:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem="project(...) source must be a Structure row or relation.",
                use="Call project(order, TargetSchema) or project(order, ['field']).",
            )

        selected = set(result.fields) if result.fields is not None else set(source_schema._structure_fields)
        unknown = selected - set(source_schema._structure_fields)
        if unknown:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"project(...) source {source_schema.__name__} has no field(s): {', '.join(sorted(unknown))}.",
                use=f"Select fields declared by {source_schema.__name__}.",
            )

        assignments: list[ProjectAssignment] = []
        for field in output_schema._structure_fields.values():
            if field.name not in selected:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=f"{output_schema.__name__}.{field.name} is not selected by project(...).",
                    use="Include the field in project(source, [...]) or use Schema.project(source)(...) with overrides.",
                    context={"field": field.name, "schema": output_schema.__name__},
                )
            expression = self._source_field(result.source, field.name)
            if expression is None:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=f"{source_schema.__name__}.{field.name} is not available for project(...).",
                    use="Use a target schema whose fields exist on the source or provide explicit overrides.",
                    context={"field": field.name, "schema": source_schema.__name__},
                )
            assignments.append(
                self._assignment(transform_class, member, output_schema, field, expression, filters=filters)
            )
        return assignments

    def _assignment(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Structure],
        field,
        expression: Expression,
        *,
        filters: tuple[Expression, ...] | list[Expression],
        allow_aggregate: bool = False,
    ) -> ProjectAssignment:
        if expression.kind == "aggregate" and not allow_aggregate:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"{output_schema.__name__}.{field.name} uses an aggregate expression outside group_by(...).",
                use="Call group_by(...) in the subtransform before returning count(), sum(...), or another aggregate.",
                context={"field": field.name, "schema": output_schema.__name__},
            )
        nullable = self._nullable(expression, filters)
        if not field.nullable and nullable:
            raise self._error(
                "SCHEMA-E0301",
                transform_class=transform_class,
                member=member,
                problem=(
                    f"{output_schema.__name__}.{field.name} is non-nullable, "
                    "but the assigned expression may produce null."
                ),
                use="Guard the source value with where(value.is_not_null()) or provide a non-null default with coalesce(...).",
                context={"field": field.name, "schema": output_schema.__name__},
            )
        if not self._assignable(expression.type, field.type, expression=expression):
            code = "SCHEMA-E0302" if self._requires_explicit_conversion(expression.type, field.type) else "SCHEMA-E0303"
            raise self._error(
                code,
                transform_class=transform_class,
                member=member,
                problem=(
                    f"{output_schema.__name__}.{field.name} expects {self._type_text(field.type)}, "
                    f"but the assigned expression is {self._type_text(expression.type)}."
                ),
                use=self._assignment_use(expression.type, field.type, field.name),
                context={
                    "field": field.name,
                    "expected": self._type_text(field.type),
                    "actual": self._type_text(expression.type),
                },
            )
        return ProjectAssignment(field=field, expression=expression)

    def _source_schema(self, source: object) -> type[Structure] | None:
        if isinstance(source, Structure):
            return type(source)
        return cast(type[Structure] | None, getattr(source, "_structure_scope_schema", None))

    def _source_field(self, source: object, field: str) -> Expression | None:
        if isinstance(source, Structure):
            if field not in source._structure_values:
                return None
            return literal(source._structure_values[field])
        try:
            return cast(Expression, getattr(source, field))
        except AttributeError:
            return None

    def _validate_joins(
        self,
        transform_class: type[Transform],
        member: str,
        joins: list,
    ) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for occurrence, join in enumerate(joins, start=1):
            if join.method is JoinMethod.LOOKUP and join.how not in {Join.LEFT, Join.INNER}:
                raise self._join_error(
                    transform_class,
                    member,
                    join.input_name,
                    occurrence,
                    f"lookup_join(...) supports Join.LEFT and Join.INNER, not {join.how!r}.",
                    "Use Join.LEFT or Join.INNER, or use rowset_join(...) for broad rowset joins.",
                )
            if join.method is JoinMethod.ROWSET and join.how not in {Join.LEFT, Join.INNER, Join.RIGHT, Join.FULL, Join.CROSS}:
                raise self._join_error(
                    transform_class,
                    member,
                    join.input_name,
                    occurrence,
                    f"rowset_join(...) does not support join type {join.how!r}.",
                    "Use Join.LEFT, Join.INNER, Join.RIGHT, Join.FULL, or Join.CROSS.",
                )
            if join.hint is not None and not isinstance(join.hint, JoinHint):
                raise self._join_error(
                    transform_class,
                    member,
                    join.input_name,
                    occurrence,
                    f"{join.method.value}(...) hint must be a JoinHint value, not {type(join.hint).__name__}.",
                    "Use JoinHint.BROADCAST or omit hint=.",
                )
            if join.strategy is not None and not isinstance(join.strategy, JoinStrategy):
                raise self._join_error(
                    transform_class,
                    member,
                    join.input_name,
                    occurrence,
                    f"{join.method.value}(...) strategy must be a JoinStrategy value, not {type(join.strategy).__name__}.",
                    "Use a JoinStrategy value or omit strategy=.",
                )
            if join.dedupe is not None:
                self._validate_join_dedupe(transform_class, member, join.input_name, occurrence, join.dedupe)
            if join.temporal is not None:
                self._validate_join_temporal(transform_class, member, join.input_name, occurrence, join.temporal)
            if join.as_of is not None:
                self._validate_join_as_of(transform_class, member, join.input_name, occurrence, join.as_of)

            if join.method is JoinMethod.ROWSET:
                conditions = self._rowset_join_conditions(
                    transform_class, member, join.input_name, occurrence, join.predicate
                )
            else:
                conditions = self._join_conditions(transform_class, member, join.input_name, occurrence, join.predicate)
            for condition in conditions:
                left, right = condition.args
                self._validate_join_pair(transform_class, member, join.input_name, occurrence, left, right)

            if (
                join.method is JoinMethod.LOOKUP
                and join.dedupe is None
                and not self._unique_join(join.input_name, join.input_schema, conditions)
            ):
                diagnostics.append(
                    Diagnostic(
                        entry=diagnostic_registry.get("JOIN-W0601"),
                        problem=f"lookup_join(...) uniqueness is not proven for input {join.input_name}.",
                        use="Mark the joined key field primary_key=True, declare a unique key, or use left_join(...) or inner_join(...) when multiplication is intended.",
                        context={"input": join.input_name, "occurrence": str(occurrence)},
                        source=f"{transform_class.__module__}.{transform_class.__name__}.{member}",
                    )
                )
        return diagnostics

    def _validate_join_temporal(
        self,
        transform_class: type[Transform],
        member: str,
        input_name: str,
        occurrence: int,
        temporal,
    ) -> None:
        if temporal.overlaps is not OverlapPolicy.ERROR:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                f"temporal_one(overlaps=...) policy {temporal.overlaps!r} is not supported.",
                "Use OverlapPolicy.ERROR or omit overlaps=.",
            )
        if input_name in self._scopes(temporal.at):
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                "temporal_one(at=...) must not read the joined temporal input.",
                "Use a current-row event time such as order.order_time.",
            )
        self._validate_temporal_bound(
            transform_class, member, input_name, occurrence, "valid_from", temporal.valid_from
        )
        self._validate_temporal_bound(transform_class, member, input_name, occurrence, "valid_to", temporal.valid_to)

    def _validate_temporal_bound(
        self,
        transform_class: type[Transform],
        member: str,
        input_name: str,
        occurrence: int,
        field: str,
        expression: Expression,
    ) -> None:
        scopes = self._scopes(expression)
        if input_name not in scopes or scopes - {input_name}:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                f"temporal_one({field}=...) must read only the joined temporal input.",
                f"Use a right-side validity field such as history.{field}.",
            )

    def _validate_join_as_of(
        self,
        transform_class: type[Transform],
        member: str,
        input_name: str,
        occurrence: int,
        as_of,
    ) -> None:
        if as_of.direction is not AsOf.BACKWARD:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                f"as_of_one(direction=...) policy {as_of.direction!r} is not supported.",
                "Use AsOf.BACKWARD or omit direction=.",
            )
        if as_of.ties is not TiePolicy.ERROR:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                f"as_of_one(ties=...) policy {as_of.ties!r} is not supported.",
                "Use TiePolicy.ERROR or omit ties=.",
            )
        if input_name in self._scopes(as_of.left_time):
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                "as_of_one(left_time=...) must not read the joined as-of input.",
                "Use a current-row event time such as trade.trade_time.",
            )
        self._validate_as_of_right_time(transform_class, member, input_name, occurrence, as_of.right_time)
        if as_of.tolerance is not None and input_name in self._scopes(as_of.tolerance):
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                "as_of_one(tolerance=...) must not read the joined as-of input.",
                "Use a literal or current-row tolerance expression.",
            )

    def _validate_as_of_right_time(
        self,
        transform_class: type[Transform],
        member: str,
        input_name: str,
        occurrence: int,
        expression: Expression,
    ) -> None:
        scopes = self._scopes(expression)
        if input_name not in scopes or scopes - {input_name}:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                "as_of_one(right_time=...) must read only the joined as-of input.",
                "Use a right-side event time such as prices.price_time.",
            )

    def _validate_join_dedupe(
        self,
        transform_class: type[Transform],
        member: str,
        input_name: str,
        occurrence: int,
        dedupe: JoinDedupe,
    ) -> None:
        if not isinstance(dedupe.ties, TiePolicy):
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                f"lookup_join(dedupe=...) ties must be a TiePolicy value, not {type(dedupe.ties).__name__}.",
                "Use TiePolicy.ERROR or omit ties=.",
            )
        if dedupe.direction not in {"latest", "earliest"}:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                f"lookup_join(dedupe=...) direction {dedupe.direction!r} is not supported.",
                "Use JoinDedupe.latest_by(...) or JoinDedupe.earliest_by(...).",
            )
        scopes = self._scopes(dedupe.order_by)
        if input_name not in scopes or scopes - {input_name}:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                "lookup_join(dedupe=...) order_by must read only the joined input.",
                "Use a right-side field such as JoinDedupe.latest_by(customer.updated_at).",
            )

    def _join_conditions(
        self,
        transform_class: type[Transform],
        member: str,
        input_name: str,
        occurrence: int,
        predicate: Expression,
    ) -> list[Expression]:
        if predicate.kind == "and":
            return [
                condition
                for argument in predicate.args
                for condition in self._join_conditions(transform_class, member, input_name, occurrence, argument)
            ]
        if predicate.kind in {"eq", "null_safe_eq"}:
            return [predicate]
        raise self._join_error(
            transform_class,
            member,
            input_name,
            occurrence,
            "v1 joins support equality key pairs combined with AND.",
            "Replace OR, inequality, or arbitrary predicates with equality pairs, or move custom join logic into a hook.",
        )

    def _rowset_join_conditions(
        self,
        transform_class: type[Transform],
        member: str,
        input_name: str,
        occurrence: int,
        predicate: Expression,
    ) -> list[Expression]:
        if predicate.kind == "literal":
            return []
        scopes = self._scopes(predicate)
        if input_name not in scopes:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                "rowset_join(...) predicate must reference the joined input.",
                "Compare the joined input with the current row or another joined scope.",
            )
        if not scopes - {input_name}:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                "rowset_join(...) predicate cannot reference only the joined input.",
                "Compare the joined input with the current row or another joined scope.",
            )
        return self._equality_conditions(predicate)

    def _equality_conditions(self, predicate: Expression) -> list[Expression]:
        if predicate.kind == "and":
            return [condition for argument in predicate.args for condition in self._equality_conditions(argument)]
        if predicate.kind in {"eq", "null_safe_eq"}:
            return [predicate]
        return []

    def _validate_join_pair(
        self,
        transform_class: type[Transform],
        member: str,
        input_name: str,
        occurrence: int,
        left: Expression,
        right: Expression,
    ) -> None:
        left_scopes = self._scopes(left)
        right_scopes = self._scopes(right)
        left_has_input = input_name in left_scopes
        right_has_input = input_name in right_scopes

        if left_has_input == right_has_input:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                "Each join key pair must compare the joined input with the current row or an earlier joined scope.",
                "Put one joined-input expression on one side of == and one non-joined expression on the other side.",
            )
        if not (left_scopes | right_scopes) - {input_name}:
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                "Join key pairs cannot compare only fields from the joined input.",
                "Compare the joined input key to the current row or a previously joined scope.",
            )
        if not self._key_compatible(left.type, right.type):
            raise self._join_error(
                transform_class,
                member,
                input_name,
                occurrence,
                f"Join key types are incompatible: {self._type_text(left.type)} and {self._type_text(right.type)}.",
                "Join fields with compatible types or use explicit expression helpers before comparing keys.",
            )

    def _unique_join(
        self,
        input_name: str,
        input_schema: type[Structure],
        conditions: list[Expression],
    ) -> bool:
        if len(conditions) != 1:
            return False
        left, right = conditions[0].args
        return self._primary_key_for_scope(left, input_name, input_schema) or self._primary_key_for_scope(
            right,
            input_name,
            input_schema,
        )

    def _primary_key_for_scope(
        self,
        expression: Expression,
        scope: str,
        schema: type[Structure],
    ) -> bool:
        if expression.kind != "field" or not expression.data or expression.data.get("scope") != scope:
            return False
        path = str(expression.data.get("name", expression.data.get("field", "")))
        if "." in path:
            return False
        field = schema._structure_fields.get(path)
        return bool(field and field.primary_key)

    def _scopes(self, expression: Expression) -> set[str]:
        scopes = set().union(*(self._scopes(argument) for argument in expression.args))
        if expression.kind == "field" and expression.data and "scope" in expression.data:
            scopes.add(str(expression.data["scope"]))
        return scopes

    def _nullable(self, expression: Expression, filters: tuple[Expression, ...] | list[Expression]) -> bool:
        if self._narrowed(expression, filters):
            return False
        if expression.kind == "field":
            return expression.nullable
        if expression.kind == "literal":
            return expression.nullable
        if expression.kind in {"is_null", "is_not_null", "null_safe_eq", "not"}:
            return False
        if expression.kind == "call":
            function = (expression.data or {}).get("function")
            if function == "coalesce":
                return all(self._nullable(argument, filters) for argument in expression.args)
            return any(self._nullable(argument, filters) for argument in expression.args)
        if expression.args:
            return any(self._nullable(argument, filters) for argument in expression.args)
        return expression.nullable

    def _narrowed(self, expression: Expression, filters: tuple[Expression, ...] | list[Expression]) -> bool:
        return any(
            filter.kind == "is_not_null" and len(filter.args) == 1 and self._same_field(expression, filter.args[0])
            for filter in filters
        )

    def _same_field(self, left: Expression, right: Expression) -> bool:
        if left.kind != "field" or right.kind != "field":
            return False
        return dict(left.data or {}) == dict(right.data or {})

    def _assignable(
        self,
        actual: StructureType | None,
        target: StructureType,
        *,
        expression: Expression,
    ) -> bool:
        if actual is None:
            return expression.kind == "literal" and (expression.data or {}).get("value") is None
        if self._same_type(actual, target):
            return True
        if target.name == "long" and actual.name == "integer":
            return True
        if target.name == "double" and actual.name in {"integer", "long", "float"}:
            return True
        if (
            target.name == "float"
            and actual.name == "double"
            and isinstance((expression.data or {}).get("value"), float)
        ):
            return True
        if isinstance(target, DecimalType):
            return self._assignable_decimal(actual, target)
        return False

    def _same_type(self, actual: StructureType, target: StructureType) -> bool:
        if actual.name != target.name:
            return False
        if isinstance(actual, DecimalType) and isinstance(target, DecimalType):
            return actual.precision == target.precision and actual.scale == target.scale
        return actual == target or actual.__class__.__name__.removesuffix("Type") == target.__class__.__name__

    def _assignable_decimal(self, actual: StructureType, target: DecimalType) -> bool:
        integer_digits = target.precision - target.scale
        if actual.name == "integer":
            return integer_digits >= 10
        if actual.name == "long":
            return integer_digits >= 19
        if isinstance(actual, DecimalType):
            return target.scale >= actual.scale and integer_digits >= actual.precision - actual.scale
        return False

    def _key_compatible(self, left: StructureType | None, right: StructureType | None) -> bool:
        if left is None or right is None:
            return False
        return self._assignable(left, right, expression=Expression(kind="field", type=left)) or self._assignable(
            right, left, expression=Expression(kind="field", type=right)
        )

    def _numeric_type(self, type: StructureType | None) -> bool:
        return type is not None and type.name in {"decimal", "double", "float", "integer", "long"}

    def _orderable_type(self, type: StructureType | None) -> bool:
        return type is not None and type.name in {"date", "decimal", "double", "float", "integer", "long", "string", "timestamp"}

    def _scalar_type(self, type: StructureType | None) -> bool:
        return type is not None and type.name not in {"array", "map", "struct"}

    def _requires_explicit_conversion(self, actual: StructureType | None, target: StructureType) -> bool:
        return (
            actual is not None
            and actual.name == "string"
            and target.name
            in {
                "decimal",
                "double",
                "float",
                "integer",
                "long",
                "date",
                "timestamp",
            }
        )

    def _assignment_use(self, actual: StructureType | None, target: StructureType, field: str) -> str:
        if self._requires_explicit_conversion(actual, target) and isinstance(target, DecimalType):
            return f"Use {field}=to_decimal(value, precision={target.precision}, scale={target.scale}) so parsing is explicit."
        if actual is not None and actual.name == "integer" and target.name == "boolean":
            return f"Use {field}=value > 0 or another explicit boolean predicate."
        return "Use a compatible Structure expression type or an explicit conversion helper."

    def _type_text(self, type: StructureType | None) -> str:
        if type is None:
            return "untyped null"
        if isinstance(type, DecimalType):
            return f"Decimal({type.precision}, {type.scale})"
        return f"{type.name}()"

    def _join_error(
        self,
        transform_class: type[Transform],
        member: str,
        input_name: str,
        occurrence: int,
        problem: str,
        use: str,
    ) -> StructureCompileError:
        return self._error(
            "JOIN-E0601",
            transform_class=transform_class,
            member=member,
            problem=problem,
            use=use,
            context={"input": input_name, "occurrence": str(occurrence)},
        )

    def _is_schema(self, value: object) -> bool:
        return isinstance(value, type) and issubclass(value, Structure)

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
            )
        )


compile_transform = CompileTransform()
