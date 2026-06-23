from __future__ import annotations

import inspect
from typing import cast, get_type_hints

from structure.app.compiler.diagnostics.api import StructureCompileError
from structure.app.compiler.ir.logic.model.HookPlan import HookPlan
from structure.app.compiler.ir.logic.model.InputPlan import InputPlan
from structure.app.compiler.ir.logic.model.OutputPlan import OutputPlan
from structure.app.compiler.ir.logic.model.ProjectAssignment import ProjectAssignment
from structure.app.compiler.ir.logic.model.StepPlan import StepPlan
from structure.app.compiler.ir.logic.model.TransformPlan import TransformPlan
from structure.app.compiler.symbolic_execution.logic.model.CompileContext import CompileContext
from structure.app.dsl.logic.model.expr.Expression import Expression
from structure.app.dsl.logic.model.expr.expressions import literal
from structure.app.dsl.logic.model.expr.RowScope import RowScope
from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.logic.model.transforms.Join import Join
from structure.app.dsl.logic.model.transforms.JoinHint import JoinHint
from structure.app.dsl.logic.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.logic.model.transforms.Transform import Transform
from structure.app.dsl.logic.model.types.DecimalType import DecimalType
from structure.app.dsl.logic.model.types.StructureType import StructureType
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
        steps, lanes, explicit_outputs, diagnostics = self._steps(transform_class, inputs)
        outputs = self._outputs(transform_class, steps, lanes, explicit_outputs)
        return TransformPlan(
            name=transform_class.__name__,
            inputs=tuple(inputs),
            steps=tuple(steps),
            outputs=tuple(outputs),
            options=dict(getattr(transform_class, "_structure_transform_options", {})),
            diagnostics=tuple(diagnostics),
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
    ) -> tuple[list[StepPlan], dict[str, dict[str, object]], set[str], list[Diagnostic]]:
        instance = transform_class()
        hooks = self._hooks(transform_class)
        steps: list[StepPlan] = []
        lanes: dict[str, dict[str, object]] = {}
        explicit_outputs: set[str] = set()
        diagnostics: list[Diagnostic] = []

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
            input_schema = cast(type[Structure], parameter.annotation)
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

            actual_schema = cast(type[Structure], input_source["schema"])
            if input_schema is not actual_schema:
                if input_lane == "df":
                    problem = (
                        f"{transform_class.__name__}.{name} expects {input_schema.__name__}, "
                        f"but the previous subtransform returns {actual_schema.__name__}."
                    )
                elif input_source.get("kind") == "input":
                    problem = (
                        f"{transform_class.__name__}.{name} expects {input_schema.__name__}, "
                        f"but input {input_lane} declares {actual_schema.__name__}."
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

            diagnostics.extend(self._validate_joins(transform_class, name, context.joins))
            assignments = self._assignments(transform_class, name, output_schema, result, filters=context.filters)
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
        return steps, lanes, explicit_outputs, diagnostics

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
            return [self._lane_output("df", steps[-1].output_schema, lanes, ordinal=0, transform_class=transform_class)]

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
