"""Conservative removal facts; unknown behavior is never certified as pure."""

from collections.abc import Iterable, Mapping
from dataclasses import fields, is_dataclass

from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe


def recipe_values(value):
    """Visit recipe payloads, including expressions stored outside argument tuples."""
    yield value
    children: Iterable[object]
    if isinstance(value, Mapping):
        children = value.values()
    elif isinstance(value, (tuple, list)):
        children = value
    elif (
        is_dataclass(value)
        and not isinstance(value, type)
        and type(value).__module__.startswith("structure.plugin.pyspark.compiler.model.")
    ):
        children = (getattr(value, item.name) for item in fields(value) if item.name != "origin")
    else:
        return
    for child in children:
        yield from recipe_values(child)


class RemovalSafety:
    _operations = frozenset({
        "filter", "join", "aggregate", "drop_duplicates", "union_all", "union_by_name", "intersect",
        "intersect_all", "except_all", "subtract", "relation_alias", "watermark",
        "explode_struct", "posexplode_struct", "explode_outer_struct", "posexplode_outer_struct",
        "explode_array", "posexplode_array", "explode_outer_array", "posexplode_outer_array",
    })
    _expressions = frozenset({
        "field", "literal", "get_field", "with_field", "drop_fields", "struct", "lambda_arg",
        "is_not_null", "is_null", "is_nan", "and", "or", "eq", "ne", "gt", "lt", "le", "ge",
        "add", "sub", "mul", "div", "mod", "neg", "when", "null_safe_eq", "isin", "not",
        "contains", "startswith", "endswith", "like", "ilike", "rlike", "item", "cast", "try_cast",
        "order", "event_time_between", "time_window", "window_time",
    })
    _functions = frozenset({
        "coalesce", "lower", "upper", "trim", "ltrim", "rtrim", "regexp_replace", "split", "length",
        "concat", "concat_ws", "abs", "sqrt", "log", "exp", "pow", "round", "floor", "ceil",
        "sum", "count", "count_distinct", "min", "max", "avg", "greatest", "least", "datediff",
        "array", "array_distinct", "array_transform", "array_filter", "size", "struct", "isnan",
    })

    def reason(self, step) -> str | None:
        hooks = (*step.before_hooks, *step.after_hooks, *(h for result in step.results for h in result.after_hooks))
        if hooks:
            return "it contains a hook"
        validations = (*step.validations, *(v for result in step.results for v in result.validations))
        if any(validation.check for validation in validations):
            return "it contains enabled schema validation"
        for operation in step.operations:
            if operation.kind in {"require_all", "require_unique", "exactly_one"} or operation.relation_assertion:
                return "it contains a required assertion"
            if operation.kind in {"checkpoint", "local_checkpoint", "cache", "persist", "unpersist"}:
                return "it contains an explicit resource lifecycle operation"
            if operation.kind not in self._operations:
                return f"unknown removal safety for operation {operation.kind}"
        joins = (*step.joins, *(op.join for op in step.operations if op.join is not None))
        if any(join.assert_singleton_in_batch for join in joins):
            return "it contains a required singleton-policy check"
        if any(join.dedupe or join.temporal or join.as_of for join in joins):
            return "unknown removal safety for a specialized join"
        aggregates = (
            step.aggregate,
            *(result.aggregate for result in step.results),
            *(op.aggregate for op in step.operations),
        )
        for aggregate in aggregates:
            if aggregate is not None and any(
                assignment.function not in {"sum", "count", "count_distinct", "min", "max", "avg"}
                or assignment.order_by is not None
                or assignment.options
                for assignment in aggregate.assignments
            ):
                return "unknown removal safety for an ordered or specialized aggregate"
        for value in recipe_values(step):
            if not isinstance(value, PySparkExpressionRecipe):
                continue
            if value.kind == "python_udf":
                return "it contains a Python UDF"
            if value.kind in {"call", "transform_expression"}:
                if value.data.get("function") not in self._functions:
                    return f"unknown removal safety for function {value.data.get('function')}"
            elif value.kind not in self._expressions:
                return f"unknown removal safety for expression {value.kind}"
            if value.kind == "field" and "column" in value.data:
                return "unknown removal safety for an opaque column"
        return None
