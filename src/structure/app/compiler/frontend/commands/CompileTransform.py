from __future__ import annotations

import inspect
from typing import cast, get_args, get_origin, get_type_hints

from structure.app.compiler.diagnostics.api import StructureCompileError
from structure.app.compiler.frontend.logic.CompilerHookCollector import CompilerHookCollector
from structure.app.compiler.frontend.logic.CompilerInputCollector import CompilerInputCollector
from structure.app.compiler.ir.model.HookPlan import HookPlan
from structure.app.compiler.ir.model.InputPlan import InputPlan
from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.compiler.ir.model.OutputPlan import OutputPlan
from structure.app.compiler.ir.model.ProjectAssignment import ProjectAssignment
from structure.app.compiler.ir.model.StepInputPlan import StepInputPlan
from structure.app.compiler.ir.model.StepPlan import StepPlan
from structure.app.compiler.ir.model.StepResultPlan import StepResultPlan
from structure.app.compiler.ir.model.TransformPlan import TransformPlan
from structure.app.compiler.symbolic_execution.model.CompileContext import CompileContext
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.expressions import literal
from structure.app.dsl.model.expr.InputScope import InputScope
from structure.app.dsl.model.expr.RowScope import RowScope
from structure.app.dsl.model.schemas.Projection import Projection
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.BindingSelector import BindingSelector
from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.Join import Join
from structure.app.dsl.model.transforms.JoinHint import JoinHint
from structure.app.dsl.model.transforms.JoinStrategy import JoinStrategy
from structure.app.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.model.transforms.reserved_v2 import reserved_operations
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.lib.cross.errors import Diagnostic, diagnostic_registry

SourceDeclaration = InputDeclaration | LaneDeclaration | BindingSelector
WriteDeclaration = LaneDeclaration | OutputDeclaration | BindingSelector


class CompileTransform:

    def __init__(self) -> None:
        self._hook_collector = CompilerHookCollector()
        self._input_collector = CompilerInputCollector()

    def __call__(self, transform_class: type[Transform]) -> TransformPlan:
        if not getattr(transform_class, "_structure_transform", False):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"{transform_class.__name__} is not decorated with @transform.",
                use="Add @transform to the class or compile a decorated Structure transform.",
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
        steps, lanes, explicit_outputs, diagnostics = self._steps(transform_class, inputs)
        outputs = self._outputs(transform_class, lanes, explicit_outputs)
        return TransformPlan(
            name=transform_class.__name__,
            inputs=tuple(inputs),
            steps=tuple(steps),
            outputs=tuple(outputs),
            options=dict(getattr(transform_class, "_structure_transform_options", {})),
            diagnostics=tuple(diagnostics),
        )

    def _steps(
        self,
        transform_class: type[Transform],
        inputs: list[InputPlan],
    ) -> tuple[list[StepPlan], dict[str, dict[str, object]], set[str], list[Diagnostic]]:
        instance = transform_class()
        hooks = self._hook_collector.collect(transform_class)
        steps: list[StepPlan] = []
        lanes: dict[str, dict[str, object]] = {}
        explicit_outputs: set[str] = set()
        diagnostics: list[Diagnostic] = []

        for name, member in transform_class.__dict__.items():
            if name.startswith("_") or name == "run" or not inspect.isfunction(member):
                continue

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
                continue
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

            context = CompileContext(step=name)
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

            context.operations.extend(reserved_operations(member))
            diagnostics.extend(self._validate_joins(transform_class, name, context.joins))
            values = self._result_values(
                transform_class,
                name,
                output_schemas,
                result,
            )
            result_plans: list[StepResultPlan] = []
            after_hooks = hooks.get(("after", name), ())
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
                        problem=(
                            f"@after({name}) replaces lane(s) that {name} does not produce: " f"{', '.join(unknown)}."
                        ),
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
                result_plans.append(
                    StepResultPlan(
                        schema=output_schema,
                        lane=output_lane,
                        frame=frame,
                        projection=tuple(
                            self._assignments(
                                transform_class,
                                name,
                                output_schema,
                                value,
                                filters=context.filters,
                            )
                        ),
                        ordinal=ordinal,
                        after_hooks=selected_hooks,
                    )
                )
            first = result_plans[0]
            driver = bindings[0]
            before_hooks = self._before_hooks(
                transform_class,
                name,
                driver.lane,
                hooks.get(("before", name), ()),
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
                    name=name,
                    input_schema=driver.schema,
                    output_schema=first.schema,
                    source=driver.source,
                    source_scope=driver.scope,
                    input_lane=driver.lane,
                    output_lane=first.lane,
                    filters=tuple(context.filters),
                    projection=first.projection,
                    ordinal=len(steps),
                    joins=tuple(context.joins),
                    operations=tuple(context.operations),
                    before_hooks=before_hooks,
                    after_hooks=first.after_hooks if len(result_plans) == 1 else (),
                    inputs=tuple(bindings),
                    results=tuple(result_plans),
                )
            )
            for item in result_plans:
                lanes[item.lane] = {
                    "schema": item.schema,
                    "source": item.frame,
                    "scope": item.schema.__name__,
                }

        if not steps:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"{transform_class.__name__} has no public schema-returning subtransform.",
                use="Add a public instance method with a Structure row parameter and Structure return annotation.",
            )
        return steps, lanes, explicit_outputs, diagnostics

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
                if operation.join.method.exposes_fields():
                    joined.add(operation.join.input_name)

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
                    use=(f"Use join_one({parameter}, on=...) " f"before reading its fields."),
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
    ) -> tuple[str, dict[str, object]]:
        if declaration is not None:
            return self._declared_source(transform_class, declaration, lanes, inputs, input_schema, member=member)
        return self._input_lane(transform_class, lanes, inputs, input_schema, member=member)

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
    ) -> tuple[str, dict[str, object]]:
        current = [(lane, source) for lane, source in lanes.items() if source["schema"] is input_schema]
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
        input_plan = self._input_for_schema(inputs, input_schema)
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

    def _input_for_schema(self, inputs: list[InputPlan], schema: type[Structure]) -> InputPlan:
        matches = [input_plan for input_plan in inputs if input_plan.schema is schema]
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
    ) -> ProjectAssignment:
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
            if join.method is JoinMethod.ONE and join.how not in {Join.LEFT, Join.INNER}:
                raise self._join_error(
                    transform_class,
                    member,
                    join.input_name,
                    occurrence,
                    f"v1 join_one(...) supports Join.LEFT and Join.INNER, not {join.how!r}.",
                    "Use Join.LEFT or Join.INNER, or move non-v1 join semantics into an explicit hook.",
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

            conditions = self._join_conditions(transform_class, member, join.input_name, occurrence, join.predicate)
            for condition in conditions:
                left, right = condition.args
                self._validate_join_pair(transform_class, member, join.input_name, occurrence, left, right)

            if join.method is JoinMethod.ONE and not self._unique_join(join.input_name, join.input_schema, conditions):
                diagnostics.append(
                    Diagnostic(
                        entry=diagnostic_registry.get("JOIN-W0601"),
                        problem=f"join_one(...) uniqueness is not proven for input {join.input_name}.",
                        use="Mark the joined key field primary_key=True, declare a unique key, or use v2 join_many(...) when multiplication is intended.",
                        context={"input": join.input_name, "occurrence": str(occurrence)},
                        source=f"{transform_class.__module__}.{transform_class.__name__}.{member}",
                    )
                )
        return diagnostics

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
