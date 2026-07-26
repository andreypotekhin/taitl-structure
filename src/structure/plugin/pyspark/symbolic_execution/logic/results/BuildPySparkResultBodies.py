from typing import cast

from structure import StructureCompileError
from structure.dsl import Schema, Transform
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.pyspark.dsl.aggregation import AggregateAssignment, AggregateKey, AggregatePlan, ProjectAssignment
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.Projection import Projection
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.dsl.types import DecimalType, Struct, StructType, StructureType
from structure.plugin.pyspark.symbolic_execution.logic.results.ValidatePySparkResultReturn import (
    ValidatePySparkResultReturn,
)
from structure.plugin.pyspark.symbolic_execution.model.PySparkResultBody import PySparkResultBody
from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import PySparkSymbolicContext


class BuildPySparkResultBodies:
    """Build PySpark projections and aggregates from a captured step return value."""

    def __init__(self, request: StepAuthoringRequest) -> None:
        self._request = request
        self._return_shape = ValidatePySparkResultReturn(request, self._raise)

    def __call__(self, value: object, *, context: PySparkSymbolicContext) -> tuple[PySparkResultBody, ...]:
        values = self._return_shape(value)
        origin = self._request.origin
        owner = getattr(origin, "owner", None)
        transform_class = owner if isinstance(owner, type) and issubclass(owner, Transform) else Transform
        member = getattr(origin, "member_name", self._request.name)
        bodies: list[PySparkResultBody] = []
        for result, result_value in zip(self._request.results, values, strict=True):
            aggregate = (
                self._aggregate_plan(
                    transform_class,
                    member,
                    cast(type[Schema], result.schema),
                    result_value,
                    keys=() if context.aggregate_keys is None else context.aggregate_keys,
                    grouping=context.aggregate_grouping,
                    levels=context.aggregate_levels,
                    having=context.aggregate_having,
                    filters=context.filters,
                )
                if context.aggregate_requested
                else None
            )
            projection = () if aggregate is not None else tuple(
                self._assignments(
                    transform_class, member, cast(type[Schema], result.schema), result_value, filters=context.filters
                )
            )
            bodies.append(PySparkResultBody(projection=projection, aggregate=aggregate))
        return tuple(bodies)

    def _raise(self, code: str, problem: str, use: str) -> None:
        raise self._error(code, transform_class=None, problem=problem, use=use)

    def _assignments(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Schema],
        result: Schema | Projection,
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
                problem=f"Step method returned {type(result).__name__}, not {output_schema.__name__}.",
                use="Return an instance of the schema declared in the step method return annotation.",
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
            expression = self._value_expression(
                transform_class,
                member,
                result._structure_values[field.name],
                field.type,
                path=f"{output_schema.__name__}.{field.name}",
                filters=filters,
            )
            assignments.append(
                self._assignment(transform_class, member, output_schema, field, expression, filters=filters)
            )
        return assignments

    def _aggregate_plan(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Schema],
        result: Schema | Projection,
        *,
        keys: tuple[tuple[str, Expression], ...],
        grouping: str,
        levels: tuple[tuple[str, ...], ...],
        having: object | None,
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
            expression = self._value_expression(
                transform_class,
                member,
                result._structure_values[field.name],
                field.type,
                path=f"{output_schema.__name__}.{field.name}",
                filters=filters,
            )
            key = self._aggregate_key_for(field.name, expression, aggregate_keys)
            if key is not None:
                self._assignment(transform_class, member, output_schema, field, expression, filters=filters)
                assignments.append(
                    AggregateAssignment(field=field, function="key", expression=expression, key=key.name)
                )
                continue
            if expression.kind == "aggregate":
                self._assignment(
                    transform_class,
                    member,
                    output_schema,
                    field,
                    expression,
                    filters=(),
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
                problem=(
                    f"{output_schema.__name__}.{field.name} is neither a grouped key nor an aggregate expression "
                    "outside group_by(...)."
                ),
                use="Assign a group_by(...) key, count(), sum(...), or a grouped parent field.",
                context={"field": field.name, "schema": output_schema.__name__},
            )
        having_expression = self._aggregate_having_expression(transform_class, member, output_schema, having)
        return AggregatePlan(
            keys=aggregate_keys,
            assignments=tuple(assignments),
            grouping=grouping,
            levels=levels,
            having=having_expression,
        )

    def _aggregate_having_expression(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Schema],
        predicate: object | None,
    ) -> Expression | None:
        if predicate is None:
            return None
        scope = RowScope(name=output_schema.__name__, schema=output_schema)
        value = predicate(scope) if callable(predicate) else predicate
        return literal(value)

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
        output_schema: type[Schema],
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
                use="Make the project(...) target match the step method return annotation.",
                context={"expected": output_schema.__name__, "actual": result.target.__name__},
            )
        source_schema = self._source_schema(result.source)
        if source_schema is None:
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem="project(...) source must be a Schema row or relation.",
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
            if isinstance(result.source, Schema):
                expression = self._value_expression(
                    transform_class,
                    member,
                    result.source._structure_values[field.name],
                    field.type,
                    path=f"{output_schema.__name__}.{field.name}",
                    filters=filters,
                )
            assignments.append(
                self._assignment(transform_class, member, output_schema, field, expression, filters=filters)
            )
        return assignments

    def _assignment(
        self,
        transform_class: type[Transform],
        member: str,
        output_schema: type[Schema],
        field,
        expression: Expression,
        *,
        filters: tuple[Expression, ...] | list[Expression],
    ) -> ProjectAssignment:
        if problem := self._comparison_problem(expression):
            raise self._error(
                "DSL-E0402",
                transform_class=transform_class,
                member=member,
                problem=problem,
                use="Compare compatible Structure values, or use an explicit cast before comparing them.",
            )
        nullable = self._nullable(expression, filters)
        if not field.nullable and nullable:
            expression_data = expression.data or {}
            nullable_reason = expression_data.get("nullable_reason")
            outer_join_detail = ""
            if isinstance(nullable_reason, str):
                outer_join_detail = (
                    f" The assigned {expression_data.get('scope')} value is nullable because {nullable_reason}."
                )
            raise self._error(
                "SCHEMA-E0301",
                transform_class=transform_class,
                member=member,
                problem=(
                    f"{output_schema.__name__}.{field.name} is non-nullable, "
                    f"but the assigned expression may produce null.{outer_join_detail}"
                ),
                use=(
                    "Use a nullable output field, guard the value with where(value.is_not_null()), "
                    "or provide a non-null default with coalesce(...)."
                ),
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

    def _value_expression(
        self,
        transform_class: type[Transform],
        member: str,
        value: object,
        target_type: StructureType,
        *,
        path: str,
        filters: tuple[Expression, ...] | list[Expression],
    ) -> Expression:
        if isinstance(value, Schema):
            expression_type = Struct(type(value))
            fields = tuple(type(value)._structure_fields.values())
            arguments: list[Expression] = []
            for nested_field in fields:
                if nested_field.name not in value._structure_values:
                    raise self._error(
                        "DSL-E0402",
                        transform_class=transform_class,
                        member=member,
                        problem=f"{path}.{nested_field.name} is not assigned.",
                        use="Assign every declared nested field when constructing a Struct(...) value.",
                        context={"field": f"{path}.{nested_field.name}", "schema": type(value).__name__},
                    )
                arguments.append(
                    self._value_expression(
                        transform_class,
                        member,
                        value._structure_values[nested_field.name],
                        nested_field.type,
                        path=f"{path}.{nested_field.name}",
                        filters=filters,
                    )
                )
            expression = Expression(
                kind="struct",
                type=expression_type,
                nullable=False,
                data={"schema": type(value), "fields": fields},
                args=tuple(arguments),
            )
            if self._same_type(expression_type, target_type):
                self._validate_struct_fields(
                    transform_class,
                    member,
                    fields,
                    expression.args,
                    path=path,
                    filters=filters,
                )
            return expression
        return literal(value)

    def _validate_struct_fields(
        self,
        transform_class: type[Transform],
        member: str,
        fields: tuple,
        expressions: tuple[Expression, ...],
        *,
        path: str,
        filters: tuple[Expression, ...] | list[Expression],
    ) -> None:
        for field, expression in zip(fields, expressions, strict=True):
            field_path = f"{path}.{field.name}"
            if problem := self._comparison_problem(expression):
                raise self._error(
                    "DSL-E0402",
                    transform_class=transform_class,
                    member=member,
                    problem=problem,
                    use="Compare compatible Structure values, or use an explicit cast before comparing them.",
                )
            if not field.nullable and self._nullable(expression, filters):
                raise self._error(
                    "SCHEMA-E0301",
                    transform_class=transform_class,
                    member=member,
                    problem=f"{field_path} is non-nullable, but the assigned expression may produce null.",
                    use=(
                        "Use a nullable nested field, guard the value with where(value.is_not_null()), "
                        "or provide a non-null default with coalesce(...)."
                    ),
                    context={"field": field_path, "schema": path},
                )
            if self._assignable(expression.type, field.type, expression=expression):
                continue
            code = "SCHEMA-E0302" if self._requires_explicit_conversion(expression.type, field.type) else "SCHEMA-E0303"
            raise self._error(
                code,
                transform_class=transform_class,
                member=member,
                problem=(
                    f"{field_path} expects {self._type_text(field.type)}, "
                    f"but the assigned expression is {self._type_text(expression.type)}."
                ),
                use=self._assignment_use(expression.type, field.type, field_path),
                context={
                    "field": field_path,
                    "expected": self._type_text(field.type),
                    "actual": self._type_text(expression.type),
                },
            )

    def _source_schema(self, source: object) -> type[Schema] | None:
        if isinstance(source, Schema):
            return type(source)
        return cast(type[Schema] | None, getattr(source, "_structure_scope_schema", None))

    def _source_field(self, source: object, field: str) -> Expression | None:
        if isinstance(source, Schema):
            if field not in source._structure_values:
                return None
            return literal(source._structure_values[field])
        try:
            return cast(Expression, getattr(source, field))
        except AttributeError:
            return None

    def _nullable(self, expression: Expression, filters: tuple[Expression, ...] | list[Expression]) -> bool:
        if self._narrowed(expression, filters):
            return False
        if expression.kind == "special_expr":
            return self._nullable(cast(Expression, (expression.data or {})["expanded"]), filters)
        if expression.kind == "field":
            parent = self._parent_field(expression)
            if parent is not None and self._narrowed(parent, filters):
                return bool((expression.data or {}).get("field_nullable", expression.nullable))
            return expression.nullable
        if expression.kind == "literal":
            return expression.nullable
        if expression.kind == "struct":
            return False
        if expression.kind in {"is_null", "is_not_null", "is_nan", "null_safe_eq"}:
            return False
        if expression.kind in {"aggregate", "transform_expression", "python_udf"}:
            return expression.nullable
        if expression.kind in {"get_field", "item", "try_cast"}:
            return expression.nullable
        if expression.kind == "when":
            _, value, fallback = expression.args
            return self._nullable(value, filters) or self._nullable(fallback, filters)
        if expression.kind == "call":
            function = (expression.data or {}).get("function")
            if function in {"coalesce", "nvl", "ifnull"}:
                return all(self._nullable(argument, filters) for argument in expression.args)
            if function == "nvl2":
                return self._nullable(expression.args[1], filters) or self._nullable(expression.args[2], filters)
            if function == "zeroifnull":
                return False
            if function == "concat_ws":
                return False
            if function == "to_decimal":
                return expression.nullable
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
        left_data = dict(left.data or {})
        right_data = dict(right.data or {})
        if left_data.get("scope") != right_data.get("scope"):
            return False
        return left_data.get("path", left_data.get("field")) == right_data.get("path", right_data.get("field"))

    def _parent_field(self, expression: Expression) -> Expression | None:
        data = dict(expression.data or {})
        path = data.get("path")
        name_path = data.get("name_path")
        if not isinstance(path, tuple) or len(path) < 2:
            return None
        parent_path = path[:-1]
        parent_name_path = (
            name_path[:-1] if isinstance(name_path, tuple) and len(name_path) == len(path) else parent_path
        )
        parent_data = dict(data)
        parent_data["field"] = ".".join(str(item) for item in parent_path)
        parent_data["name"] = ".".join(str(item) for item in parent_name_path)
        parent_data["path"] = parent_path
        parent_data["name_path"] = parent_name_path
        parent_data.pop("field_nullable", None)
        return Expression(kind="field", type=None, nullable=True, data=parent_data)

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
        if isinstance(actual, StructType) and isinstance(target, StructType):
            return actual.schema is target.schema
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

    def _comparison_problem(self, expression: Expression) -> str | None:
        data = expression.data or {}
        problem = data.get("comparison_problem")
        if isinstance(problem, str):
            return problem
        return next((problem for argument in expression.args if (problem := self._comparison_problem(argument))), None)

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
        if isinstance(type, StructType):
            return f"Struct({type.schema.__name__})"
        return f"{type.name}()"

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
        origin = self._request.origin
        source = (
            f"{getattr(origin, 'module', '')}.{getattr(origin, 'class_name', 'Transform')}."
            f"{member or self._request.name}"
        ).lstrip(".")
        return StructureCompileError(
            Diagnostic(
                entry=diagnostic_registry.get(code),
                problem=problem,
                use=use,
                context=context or {},
                source=source,
                primary_span=self._request.primary_span,
            )
        )
