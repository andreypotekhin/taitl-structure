from structure import StructureCompileError
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.pyspark.dsl.aggregation.AggregatePlan import AggregatePlan
from structure.plugin.pyspark.dsl.types import BooleanType, StructureType
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class ValidatePySparkAggregates:
    """Validate PySpark aggregate function input types from captured bodies."""

    def __call__(self, body: PySparkStepBody, *, request: StepAuthoringRequest) -> None:
        for result, body_result in zip(request.results, body.results, strict=True):
            if body_result.aggregate is not None:
                self._plan(body_result.aggregate, schema=result.schema, request=request)

    def _plan(self, aggregate: AggregatePlan, *, schema: object, request: StepAuthoringRequest) -> None:
        schema_name = getattr(schema, "__name__", "Result")
        self._grouping_sets(aggregate, schema_name=schema_name, request=request)
        self._having(aggregate, schema_name=schema_name, request=request)
        for assignment in aggregate.assignments:
            function = assignment.function
            arguments = assignment.arguments
            argument = arguments[0] if arguments else None
            # ``first`` is the compiler's implicit representative-value
            # aggregate for a nested field beneath a grouping key.  Unlike the
            # user-facing ``first_value`` operation it deliberately supports
            # every PySpark field type, including structs.
            if function in {"key", "count", "grouping_id", "first"} or (
                function == "is_grouped" and argument is not None
            ):
                continue
            if argument is None:
                self._error(request, schema_name, assignment.field.name, function, "an input expression")
                continue
            numeric = {
                "avg",
                "sum",
                "stddev",
                "variance",
                "corr",
                "covar",
                "approx_percentile",
                "percentile",
                "skewness",
                "kurtosis",
            }
            if function in numeric and not all(self._numeric(item.type) for item in arguments):
                self._error(request, schema_name, assignment.field.name, function, "a numeric expression")
            deterministic_mode = function == "mode" and dict(assignment.options).get("deterministic") is True
            if (
                function in {"max", "min", "first_value", "last_value"} or deterministic_mode
            ) and not self._orderable(argument.type):
                self._error(request, schema_name, assignment.field.name, function, "an orderable scalar expression")
            if function in {"count_distinct", "approx_count_distinct"} and not self._scalar(argument.type):
                self._error(request, schema_name, assignment.field.name, function, "a scalar expression")
            if function in {"bool_and", "bool_or"} and not isinstance(argument.type, BooleanType):
                self._error(request, schema_name, assignment.field.name, function, "a Boolean expression")

    def _grouping_sets(self, aggregate: AggregatePlan, *, schema_name: str, request: StepAuthoringRequest) -> None:
        if aggregate.grouping != "grouping_sets":
            return
        for assignment in aggregate.assignments:
            if assignment.function != "key" or assignment.key is None or assignment.field.nullable:
                continue
            if any(assignment.key not in level for level in aggregate.levels):
                origin = request.origin
                member = getattr(origin, "member_name", request.name)
                source = (
                    f"{getattr(origin, 'module', '')}.{getattr(origin, 'class_name', 'Transform')}.{member}".lstrip(".")
                )
                raise StructureCompileError(
                    Diagnostic(
                        entry=diagnostic_registry.get("SCHEMA-E0301"),
                        problem=(
                            f"{schema_name}.{assignment.field.name} is non-nullable, but grouping_sets(...) "
                            f"omits {assignment.key} in at least one level."
                        ),
                        use="Make subtotal grouping-key fields nullable, or remove grouping levels that omit the key.",
                        context={"field": assignment.field.name, "schema": schema_name, "grouping_key": assignment.key},
                        source=source,
                    )
                )

    def _having(self, aggregate: AggregatePlan, *, schema_name: str, request: StepAuthoringRequest) -> None:
        expression = aggregate.having
        if expression is None:
            return
        if not isinstance(expression.type, BooleanType):
            self._aggregate_error(
                request,
                schema_name,
                f"{schema_name} having(...) predicate is not Boolean.",
                "Pass a predicate such as lambda out: out.order_count > 0.",
            )
        scopes = self._scopes(expression)
        if scopes <= {schema_name}:
            return
        names = ", ".join(sorted(scopes - {schema_name}))
        self._aggregate_error(
            request,
            schema_name,
            f"{schema_name} having(...) reads pre-aggregate or unrelated field scope(s): {names}.",
            "having(...) is evaluated after aggregation; reference aggregate output fields through the callback argument, for example lambda out: out.order_count > 0.",
            scopes=", ".join(sorted(scopes)),
        )

    def _scopes(self, expression) -> set[str]:
        scopes = set().union(*(self._scopes(argument) for argument in expression.args))
        if expression.kind == "field" and expression.data and "scope" in expression.data:
            scopes.add(str(expression.data["scope"]))
        return scopes

    def _aggregate_error(
        self, request: StepAuthoringRequest, schema: str, problem: str, use: str, **context: str
    ) -> None:
        origin = request.origin
        member = getattr(origin, "member_name", request.name)
        source = f"{getattr(origin, 'module', '')}.{getattr(origin, 'class_name', 'Transform')}.{member}".lstrip(".")
        raise StructureCompileError(
            Diagnostic(
                entry=diagnostic_registry.get("DSL-E0402"),
                problem=problem,
                use=use,
                context={"schema": schema, **context},
                source=source,
            )
        )

    def _numeric(self, type: StructureType | None) -> bool:
        return type is not None and type.name in {"decimal", "double", "float", "integer", "long"}

    def _orderable(self, type: StructureType | None) -> bool:
        return type is not None and type.name in {
            "date",
            "decimal",
            "double",
            "float",
            "integer",
            "long",
            "string",
            "timestamp",
        }

    def _scalar(self, type: StructureType | None) -> bool:
        return type is not None and type.name not in {"array", "map", "struct"}

    def _error(self, request: StepAuthoringRequest, schema: str, field: str, function: str, expected: str) -> None:
        origin = request.origin
        member = getattr(origin, "member_name", request.name)
        source = f"{getattr(origin, 'module', '')}.{getattr(origin, 'class_name', 'Transform')}.{member}".lstrip(".")
        raise StructureCompileError(
            Diagnostic(
                entry=diagnostic_registry.get("DSL-E0402"),
                problem=f"{schema}.{field} uses {function}(...) with unsupported input type.",
                use=f"Pass {expected} to {function}(...), or move custom aggregation logic into an explicit hook.",
                context={"field": field, "schema": schema, "function": function},
                source=source,
            )
        )
