from __future__ import annotations

import inspect
from typing import get_type_hints

from structure.app.dsl.logic.model.expr.expressions import literal
from structure.app.dsl.logic.model.expr.RowScope import RowScope
from structure.app.dsl.logic.model.plans.HookPlan import HookPlan
from structure.app.dsl.logic.model.plans.InputPlan import InputPlan
from structure.app.dsl.logic.model.plans.ProjectAssignment import ProjectAssignment
from structure.app.dsl.logic.model.plans.StepPlan import StepPlan
from structure.app.dsl.logic.model.plans.TransformPlan import TransformPlan
from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.transforms.CompileContext import CompileContext
from structure.app.dsl.logic.model.transforms.Transform import Transform


class CompileTransform:

    def __call__(self, transform_class: type[Transform]) -> TransformPlan:
        if not getattr(transform_class, "_structure_transform", False):
            raise TypeError(f"{transform_class.__name__} is not decorated with @transform")

        inputs = self._inputs(transform_class)
        steps = self._steps(transform_class, inputs)
        return TransformPlan(
            name=transform_class.__name__,
            inputs=tuple(inputs),
            steps=tuple(steps),
            options=dict(getattr(transform_class, "_structure_transform_options", {})),
        )

    def _inputs(self, transform_class: type[Transform]) -> list[InputPlan]:
        inputs: list[InputPlan] = []
        for ordinal, declaration in enumerate(transform_class._structure_inputs.values()):
            inputs.append(InputPlan(name=declaration.name, schema=declaration.schema, ordinal=ordinal))
        return inputs

    def _steps(self, transform_class: type[Transform], inputs: list[InputPlan]) -> list[StepPlan]:
        instance = transform_class()
        hooks = self._hooks(transform_class)
        steps: list[StepPlan] = []
        current_schema: type[Structure] | None = None

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
            if current_schema is not None and input_schema is not current_schema:
                raise TypeError(
                    f"{transform_class.__name__}.{name} expects {input_schema.__name__}, not {current_schema.__name__}"
                )

            input_plan = self._input_for_schema(inputs, input_schema) if current_schema is None else None
            scope_name = input_plan.name if input_plan else input_schema.__name__
            row = RowScope(name=scope_name, schema=input_schema)
            context = CompileContext(step=name)

            with context:
                result = member(instance, row)

            assignments = self._assignments(output_schema, result)
            steps.append(
                StepPlan(
                    name=name,
                    input_schema=input_schema,
                    output_schema=output_schema,
                    filters=tuple(context.filters),
                    projection=tuple(assignments),
                    ordinal=len(steps),
                    joins=tuple(context.joins),
                    before_hooks=hooks.get(("before", name), ()),
                    after_hooks=hooks.get(("after", name), ()),
                )
            )
            current_schema = output_schema

        if not steps:
            raise TypeError(f"{transform_class.__name__} has no public schema-returning subtransform")
        return steps

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
            raise TypeError(f"{method.__qualname__} must declare exactly one row parameter")

        parameter = row_parameters[0]
        annotation = hints.get(parameter.name)
        if not self._is_schema(annotation):
            raise TypeError(f"{method.__qualname__}.{parameter.name} must be annotated with a Structure schema")

        return parameter.replace(annotation=annotation)

    def _input_for_schema(self, inputs: list[InputPlan], schema: type[Structure]) -> InputPlan:
        matches = [input_plan for input_plan in inputs if input_plan.schema is schema]
        if len(matches) != 1:
            raise TypeError(f"Expected exactly one declared input for schema {schema.__name__}")
        return matches[0]

    def _assignments(self, output_schema: type[Structure], result: Structure) -> list[ProjectAssignment]:
        if not isinstance(result, output_schema):
            raise TypeError(f"Subtransform returned {type(result).__name__}, not {output_schema.__name__}")

        assignments: list[ProjectAssignment] = []
        for field in output_schema._structure_fields.values():
            if field.name not in result._structure_values:
                raise TypeError(f"{output_schema.__name__}.{field.name} is not assigned")
            assignments.append(ProjectAssignment(field=field, expression=literal(result._structure_values[field.name])))
        return assignments

    def _is_schema(self, value: object) -> bool:
        return isinstance(value, type) and issubclass(value, Structure)


compile_transform = CompileTransform()
