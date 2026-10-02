from typing import cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.expressions import make_date
from structure.plugin.pyspark.dsl.types import DateType
from structure.plugin.pyspark.execution.logic.expressions.EvaluatePySparkExpression import EvaluatePySparkExpression


class DateParts(Schema):
    year = integer(nullable=False)
    month = long(nullable=True)
    day = integer(nullable=False)


class BuiltDate(Schema):
    value = date(nullable=True)


@transform
class BuildDate(Transform):
    source = input(DateParts)
    target = output(BuiltDate)

    def convert(self, row: DateParts) -> BuiltDate:
        return BuiltDate(value=make_date(row.year, row.month, row.day))


def _expression():
    recipe = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(BuildDate, materialize_schemas=False).lowered,
    )
    return recipe.steps[0].projection[0].expression


def test_v6_make_date_uses_typed_integral_columns_and_nullable_date_result() -> None:
    expression = _expression()

    assert expression.type == DateType()
    assert expression.nullable is True
    assert PySpark.render.expression()(expression, scope_aliases={"source": "source"}) == (
        'F.make_date(F.col("source.year"), F.col("source.month"), F.col("source.day"))'
    )


def test_v6_make_date_online_evaluation_preserves_component_columns() -> None:
    expression = _expression()

    class Functions:
        @staticmethod
        def col(name):
            return name

        @staticmethod
        def make_date(*components):
            return components

    assert EvaluatePySparkExpression().evaluate(
        expression,
        functions=Functions(),
        aliases={"source": "source"},
    ) == ("source.year", "source.month", "source.day")


def test_v6_make_date_rejects_non_integral_structure_expressions() -> None:
    class InvalidDateParts(Schema):
        year = string(nullable=False)
        month = integer(nullable=False)
        day = integer(nullable=False)

    @transform
    class InvalidDate(Transform):
        source = input(InvalidDateParts)
        target = output(BuiltDate)

        def convert(self, row: InvalidDateParts) -> BuiltDate:
            return BuiltDate(value=make_date(row.year, row.month, row.day))

    with pytest.raises(TypeError, match="make_date"):
        Compiler.frontend.compile()(InvalidDate, materialize_schemas=False)
