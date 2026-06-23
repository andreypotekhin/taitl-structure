from __future__ import annotations

import inspect
from typing import get_type_hints

from structure.app.compiler.diagnostics.api import StructureCompileError
from structure.app.dsl.logic.model.expr.expressions import literal
from structure.app.dsl.logic.model.expr.RowScope import RowScope
from structure.app.dsl.logic.model.plans.HookPlan import HookPlan
from structure.app.dsl.logic.model.plans.InputPlan import InputPlan
from structure.app.dsl.logic.model.plans.OutputPlan import OutputPlan
from structure.app.dsl.logic.model.plans.ProjectAssignment import ProjectAssignment
from structure.app.dsl.logic.model.plans.StepPlan import StepPlan
from structure.app.dsl.logic.model.plans.TransformPlan import TransformPlan
from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.transforms.CompileContext import CompileContext
from structure.app.dsl.logic.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.logic.model.transforms.Transform import Transform
from structure.lib.cross.errors import Diagnostic, diagnostic_registry


class CompileTransform:

    def __call__(self, transform_class: type[Transform]) -> TransformPlan:
        if not getattr(transform_class, "_structure_transform", False):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"{transform_class.__name__} is not decorated with @transform.",
                use="Add @transform to the class or compile a decorated Structure transform.",
            )

        inputs = self._inputs(transform_class)
        steps, lanes, explicit_outputs = self._steps(transform_class, inputs)
        outputs = self._outputs(transform_class, steps, lanes, explicit_outputs)
        return TransformPlan(
            name=transform_class.__name__,
            inputs=tuple(inputs),
            steps=tuple(steps),
            outputs=tuple(outputs),
            options=dict(getattr(transform_class, "_structure_transform_options", {})),
        )

    def _inputs(self, transform_class: type[Transform]) -> list[InputPlan]:
        inputs: list[InputPlan] = []
        for ordinal, declaration in enumerate(transform_class._structure_inputs.values()):
            inputs.append(InputPlan(name=declaration.name, schema=declaration.schema, ordinal=ordinal))
        return inputs

    def _steps(
        self,
        transform_class: type[Transform],
        inputs: list[InputPlan],
    ) -> tuple[list[StepPlan], dict[str, dict[str, object]], set[str]]:
        instance = transform_class()
        hooks = self._hooks(transform_class)
        steps: list[StepPlan] = []
        lanes: dict[str, dict[str, object]] = {}
        explicit_outputs: set[str] = set()

        for name, member in transform_class.__dict__.items():
            if name.startswith("_") or name == "run" or not inspect.isfunction(member):
                continue

            hints = get_type_hints(member)
            output_schema = hints.get("return")
            if not self._is_schema(output_schema):
                continue
            assert isinstance(output_schema, type)
            assert issubclass(output_schema, Structure)

            parameter = self._row_parameter(member, hints)
            input_schema = parameter.annotation
            metadata = getattr(member, "_structure_output_method", None)
            input_lane, input_source = self._input_lane(
                transform_class,
                metadata,
                lanes,
                inputs,
                input_schema,
                member=name,
            )
            output_lane = self._output_lane(
                transform_class,
                metadata,
                output_schema,
                member=name,
                explicit_outputs=explicit_outputs,
            )

            actual_schema = input_source["schema"]
            if input_schema is not actual_schema:
                if input_lane == "df":
                    problem = (
                        f"{transform_class.__name__}.{name} expects {input_schema.__name__}, "
                        f"but the previous subtransform returns {actual_schema.__name__}."
                    )
                else:
                    problem = (
                        f"{transform_class.__name__}.{name} expects {input_schema.__name__}, "
                        f"but lane {input_lane} currently carries {actual_schema.__name__}."
                    )
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=name,
                    problem=problem,
                    use="Reorder subtransforms or update the row parameter annotation to match the selected input lane.",
                    context={"expected": input_schema.__name__, "actual": actual_schema.__name__},
                )

            row = RowScope(name=str(input_source["scope"]), schema=input_schema)
            context = CompileContext(step=name)

            try:
                with context:
                    result = member(instance, row)
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

            assignments = self._assignments(transform_class, name, output_schema, result)
            steps.append(
                StepPlan(
                    name=name,
                    input_schema=input_schema,
                    output_schema=output_schema,
                    source=str(input_source["source"]),
                    source_scope=str(input_source["scope"]),
                    input_lane=input_lane,
                    output_lane=output_lane,
                    filters=tuple(context.filters),
                    projection=tuple(assignments),
                    ordinal=len(steps),
                    joins=tuple(context.joins),
                    before_hooks=hooks.get(("before", name), ()),
                    after_hooks=hooks.get(("after", name), ()),
                )
            )
            lanes[output_lane] = {
                "schema": output_schema,
                "source": name,
                "scope": output_schema.__name__,
            }

        if not steps:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"{transform_class.__name__} has no public schema-returning subtransform.",
                use="Add a public instance method with a Structure row parameter and Structure return annotation.",
            )
        return steps, lanes, explicit_outputs

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
            return lane, {"schema": input_plan.schema, "source": input_plan.name, "scope": input_plan.name}

        if not isinstance(declaration, OutputDeclaration):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem="@transform(input=...) must reference an output(...) field.",
                use="Pass a class field declared as name = output(Schema).",
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

        declaration = metadata["output"]
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
        steps: list[StepPlan],
        lanes: dict[str, dict[str, object]],
        explicit_outputs: set[str],
    ) -> list[OutputPlan]:
        declarations = list(transform_class._structure_outputs.values())
        options = dict(getattr(transform_class, "_structure_transform_options", {}))
        class_output = options.get("to")

        if class_output is not None and declarations:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                problem=f"{transform_class.__name__} mixes @transform(to=...) with field-declared outputs.",
                use="Use either @transform(to=Schema) for a single unnamed result or output(...) fields for named results.",
            )

        if class_output is not None:
            assert isinstance(class_output, type)
            assert issubclass(class_output, Structure)
            return [self._lane_output("df", class_output, lanes, ordinal=0, transform_class=transform_class)]

        if not declarations:
            return [
                self._lane_output("df", steps[-1].output_schema, lanes, ordinal=0, transform_class=transform_class)
            ]

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
        actual_schema = source["schema"]
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

    def _hooks(self, transform_class: type[Transform]) -> dict[tuple[str, str], tuple[HookPlan, ...]]:
        grouped: dict[tuple[str, str], list[HookPlan]] = {}
        for name, member in transform_class.__dict__.items():
            metadata = getattr(member, "_structure_hook", None)
            if metadata is None:
                continue

            key = (metadata["phase"], metadata["target"])
            grouped.setdefault(key, []).append(
                HookPlan(
                    name=name,
                    phase=metadata["phase"],
                    target=metadata["target"],
                    pass_inputs=metadata["pass_inputs"],
                    schema_mode=metadata["schema_mode"],
                    project_output=metadata["project_output"],
                    streaming_safe=metadata["streaming_safe"],
                )
            )
        return {key: tuple(value) for key, value in grouped.items()}

    def _row_parameter(self, method, hints: dict[str, object]) -> inspect.Parameter:
        parameters = list(inspect.signature(method).parameters.values())
        row_parameters = [parameter for parameter in parameters if parameter.name != "self"]
        if len(row_parameters) != 1:
            raise self._error(
                "DSL-E0402",
                transform_class=None,
                member=method.__qualname__,
                problem=f"{method.__qualname__} must declare exactly one row parameter.",
                use="Declare one non-self row parameter annotated with the input or previous output schema.",
            )

        parameter = row_parameters[0]
        annotation = hints.get(parameter.name)
        if not self._is_schema(annotation):
            raise self._error(
                "DSL-E0402",
                transform_class=None,
                member=method.__qualname__,
                problem=f"{method.__qualname__}.{parameter.name} must be annotated with a Structure schema.",
                use="Annotate the row parameter with a Structure schema class.",
                context={"parameter": parameter.name},
            )

        return parameter.replace(annotation=annotation)

    def _input_for_schema(self, inputs: list[InputPlan], schema: type[Structure]) -> InputPlan:
        matches = [input_plan for input_plan in inputs if input_plan.schema is schema]
        if len(matches) != 1:
            raise self._error(
                "DSL-E0402",
                transform_class=None,
                problem=f"Expected exactly one declared input for schema {schema.__name__}.",
                use="Declare exactly one input(...) for the first subtransform's input schema.",
                context={"schema": schema.__name__, "matches": str(len(matches))},
            )
        return matches[0]

    def _assignments(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Structure],
        result: Structure,
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
            assignments.append(ProjectAssignment(field=field, expression=literal(result._structure_values[field.name])))
        return assignments

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
