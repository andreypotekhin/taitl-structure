from __future__ import annotations

import inspect
from typing import cast, get_args, get_origin, get_type_hints

from structure.app.compiler.diagnostics.api import StructureCompileError
from structure.app.compiler.frontend.logic.CompilerHookCollector import CompilerHookCollector
from structure.app.compiler.frontend.logic.CompilerInputCollector import CompilerInputCollector
from structure.app.compiler.ir.model.HookPlan import HookPlan
from structure.app.compiler.ir.model.InputPlan import InputPlan
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
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.Join import Join
from structure.app.dsl.model.transforms.JoinHint import JoinHint
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.dsl.model.types.DecimalType import DecimalType
from structure.app.dsl.model.types.StructureType import StructureType
from structure.lib.cross.errors import Diagnostic, diagnostic_registry


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
                output_schemas,
                member=name,
                explicit_outputs=explicit_outputs,
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
                if hook.df is not None and hook.df.name not in output_lanes:
                    raise self._error(
                        "DSL-E0402",
                        transform_class=transform_class,
                        member=hook.name,
                        problem=(
                            f"@after({name}, df={hook.df.name}) selects an output " f"that {name} does not produce."
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
                frame = output_lane if len(output_schemas) > 1 else name
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
            self._validate_relation_reads(
                transform_class,
                name,
                bindings,
                context.joins,
                context.filters,
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
                    before_hooks=hooks.get(("before", name), ()),
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
        joins: list,
        filters: list[Expression],
        results: list[StepResultPlan],
    ) -> None:
        joined = {join.input_name for join in joins}
        reads = set().union(
            *(self._scopes(expression) for expression in filters),
            *(self._scopes(assignment.expression) for result in results for assignment in result.projection),
        )
        for binding in bindings[1:]:
            if binding.scope in reads and binding.scope not in joined:
                raise self._error(
                    "JOIN-E0601",
                    transform_class=transform_class,
                    member=member,
                    problem=(
                        f"{transform_class.__name__}.{member} reads relation parameter "
                        f"{binding.parameter} before it is joined."
                    ),
                    use=(
                        f"Use {binding.parameter} = join_one({binding.parameter}, on=...) "
                        f"before reading its fields."
                    ),
                    context={"input": binding.parameter},
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
        declarations = (
            cast(tuple[InputDeclaration | OutputDeclaration, ...], metadata.get("inputs", ())) if metadata else ()
        )
        if len(parameters) == 1:
            parameter = parameters[0]
            schema = cast(type[Structure], parameter.annotation)
            lane, source = self._input_lane(
                transform_class,
                metadata,
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
        if declarations and len(declarations) != len(parameters):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=(
                    f"@transform(inputs=...) binds {len(declarations)} source(s), "
                    f"but {transform_class.__name__}.{member} declares {len(parameters)} schema parameter(s)."
                ),
                use="List one input(...) or available output(...) declaration for every schema parameter, in order.",
            )

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
                    use="Bind each schema parameter to a distinct input or available output lane.",
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
        declaration: object,
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        schema: type[Structure],
        *,
        member: str,
        driving: bool,
        used: set[tuple[str, str]],
    ) -> tuple[str, dict[str, object]]:
        if declaration is not None:
            return self._input_lane(
                transform_class,
                {"input": declaration},
                lanes,
                inputs,
                schema,
                member=member,
            )
        if driving and "df" in lanes and lanes["df"]["schema"] is schema:
            return "df", lanes["df"]

        candidates: list[tuple[str, dict[str, object]]] = []
        for input_plan in inputs:
            source = {
                "kind": "input",
                "schema": input_plan.schema,
                "source": input_plan.name,
                "scope": input_plan.name,
            }
            if input_plan.schema is schema and (input_plan.name, input_plan.name) not in used:
                candidates.append((input_plan.name, source))
        for lane, source in lanes.items():
            key = (lane, str(source["source"]))
            if lane != "df" and source["schema"] is schema and key not in used:
                candidates.append((lane, source))
        if len(candidates) != 1:
            names = ", ".join(lane for lane, _ in candidates) or "none"
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"Cannot deduce parameter source for schema {schema.__name__}; matched sources: {names}.",
                use="Add @transform(inputs=[...]) with one declaration for every schema parameter, in method order.",
                context={"schema": schema.__name__, "matches": str(len(candidates))},
            )
        return candidates[0]

    def _output_lanes(
        self,
        transform_class: type[Transform],
        metadata: dict[str, object] | None,
        output_schemas: tuple[type[Structure], ...],
        *,
        member: str,
        explicit_outputs: set[str],
    ) -> tuple[str, ...]:
        declarations = cast(tuple[OutputDeclaration, ...], metadata.get("outputs", ())) if metadata else ()
        if len(output_schemas) == 1:
            return (
                self._output_lane(
                    transform_class,
                    metadata,
                    output_schemas[0],
                    member=member,
                    explicit_outputs=explicit_outputs,
                ),
            )
        if declarations:
            if len(declarations) != len(output_schemas):
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=(
                        f"@transform(outputs=...) binds {len(declarations)} output(s), "
                        f"but {transform_class.__name__}.{member} returns {len(output_schemas)} schema value(s)."
                    ),
                    use="List one output(...) declaration for every returned schema, in order.",
                )
            lanes: list[str] = []
            for schema, declaration in zip(output_schemas, declarations, strict=True):
                assert isinstance(declaration, OutputDeclaration)
                self._declared_output(transform_class, declaration, member=member, role="output")
                if schema is not declaration.schema:
                    raise self._error(
                        "DSL-E0402",
                        transform_class=transform_class,
                        member=member,
                        problem=(
                            f"Result {len(lanes)} returns {schema.__name__}, "
                            f"not output {declaration.name}'s {declaration.schema.__name__}."
                        ),
                        use="Order outputs=[...] to match the tuple return annotation.",
                    )
                lanes.append(declaration.name)
                explicit_outputs.add(declaration.name)
            return tuple(lanes)
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
                    use="Add @transform(outputs=[...]) with one output declaration for every result, in return order.",
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
    ) -> tuple[Structure, ...]:
        if len(schemas) == 1:
            return (cast(Structure, result),)
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
        return cast(tuple[Structure, ...], result)

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
            if not multiple:
                if hook.df is None or hook.df.name == lane:
                    selected.append(hook)
                continue
            if hook.df is None:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=hook.name,
                    problem=f"@after({member}) is ambiguous because {member} returns multiple DataFrames.",
                    use=f"Select one result with @after({member}, df=output_declaration).",
                )
            self._declared_output(transform_class, hook.df, member=hook.name, role="df")
            if hook.df.name == lane:
                selected.append(hook)
        return tuple(selected)

    def _input_lane(
        self,
        transform_class: type[Transform],
        metadata: dict[str, object] | None,
        lanes: dict[str, dict[str, object]],
        inputs: list[InputPlan],
        input_schema: type[Structure],
        *,
        member: str,
    ) -> tuple[str, dict[str, object]]:
        declaration = metadata.get("input") if metadata else None
        if declaration is None:
            lane = "df"
            if lane in lanes:
                return lane, lanes[lane]
            input_plan = self._input_for_schema(inputs, input_schema)
            return lane, {
                "kind": "input",
                "schema": input_plan.schema,
                "source": input_plan.name,
                "scope": input_plan.name,
            }

        if isinstance(declaration, InputDeclaration):
            self._declared_input(transform_class, declaration, member=member)
            return declaration.name, {
                "kind": "input",
                "schema": declaration.schema,
                "source": declaration.name,
                "scope": declaration.name,
            }

        if not isinstance(declaration, OutputDeclaration):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem="@transform(input=...) must reference an input(...) or output(...) field.",
                use="Pass a class field declared as name = input(Schema) or name = output(Schema).",
            )
        self._declared_output(transform_class, declaration, member=member, role="input")
        lane = declaration.name
        if lane not in lanes:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"Input lane {lane} is not available yet.",
                use="Consume only lanes produced earlier in source order, or omit input= to read the canonical df lane.",
                context={"lane": lane},
            )
        return lane, lanes[lane]

    def _output_lane(
        self,
        transform_class: type[Transform],
        metadata: dict[str, object] | None,
        output_schema: type[Structure],
        *,
        member: str,
        explicit_outputs: set[str],
    ) -> str:
        if metadata is None:
            return "df"

        declaration = metadata.get("output")
        if declaration is None:
            return "df"
        assert isinstance(declaration, OutputDeclaration)
        self._declared_output(transform_class, declaration, member=member, role="output")
        if output_schema is not declaration.schema:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=f"{transform_class.__name__}.{member} returns {output_schema.__name__}, not {declaration.schema.__name__}.",
                use="Return the schema declared by the bound output(...) field.",
                context={"expected": declaration.schema.__name__, "actual": output_schema.__name__},
            )
        explicit_outputs.add(declaration.name)
        return declaration.name

    def _outputs(
        self,
        transform_class: type[Transform],
        lanes: dict[str, dict[str, object]],
        explicit_outputs: set[str],
    ) -> list[OutputPlan]:
        declarations = list(transform_class._structure_outputs.values())

        if len(declarations) == 1 and not explicit_outputs:
            declaration = declarations[0]
            return [
                self._lane_output(
                    declaration.name,
                    declaration.schema,
                    {declaration.name: lanes["df"]},
                    ordinal=0,
                    transform_class=transform_class,
                )
            ]

        outputs: list[OutputPlan] = []
        for ordinal, declaration in enumerate(declarations):
            if declaration.name not in explicit_outputs:
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    problem=f"Output {declaration.name} has no explicit transform method.",
                    use=f"Add @transform(output={declaration.name}) to the method that produces this output lane.",
                    context={"output": declaration.name},
                )
            outputs.append(
                self._lane_output(
                    declaration.name,
                    declaration.schema,
                    lanes,
                    ordinal=ordinal,
                    transform_class=transform_class,
                )
            )
        return outputs

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
        result: Structure,
        *,
        filters: tuple[Expression, ...] | list[Expression],
    ) -> list[ProjectAssignment]:
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
                code = (
                    "SCHEMA-E0302"
                    if self._requires_explicit_conversion(expression.type, field.type)
                    else "SCHEMA-E0303"
                )
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
            assignments.append(ProjectAssignment(field=field, expression=expression))
        return assignments

    def _validate_joins(
        self,
        transform_class: type[Transform],
        member: str,
        joins: list,
    ) -> list[Diagnostic]:
        diagnostics: list[Diagnostic] = []
        for occurrence, join in enumerate(joins, start=1):
            if join.how not in {Join.LEFT, Join.INNER}:
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
                    f"join_one(...) hint must be a JoinHint value, not {type(join.hint).__name__}.",
                    "Use JoinHint.BROADCAST or omit hint=.",
                )

            conditions = self._join_conditions(transform_class, member, join.input_name, occurrence, join.predicate)
            for condition in conditions:
                left, right = condition.args
                self._validate_join_pair(transform_class, member, join.input_name, occurrence, left, right)

            if not self._unique_join(join.input_name, join.input_schema, conditions):
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
        path = str(expression.data.get("field", ""))
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
