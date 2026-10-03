from typing import cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.expressions import to_timestamp_ntz
from structure.plugin.pyspark.dsl.types import TimestampNTZType
from structure.plugin.pyspark.execution.logic.expressions.EvaluatePySparkExpression import EvaluatePySparkExpression


class TimestampText(Schema):
    raw = string(nullable=True)
    pattern = string(nullable=False)


class ParsedTimestamp(Schema):
    value = timestamp_ntz(nullable=True)


@transform
class ParseTimestamp(Transform):
    source = input(TimestampText)
    target = output(ParsedTimestamp)

    def convert(self, row: TimestampText) -> ParsedTimestamp:
        return ParsedTimestamp(value=to_timestamp_ntz(row.raw, format=row.pattern))


def _expression():
    recipe = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(ParseTimestamp, materialize_schemas=False).lowered,
    )
    return recipe.steps[0].projection[0].expression


def test_v6_to_timestamp_ntz_preserves_dynamic_format_and_fixed_result_type() -> None:
    expression = _expression()

    assert expression.type == TimestampNTZType()
    assert expression.nullable is True
    assert PySpark.render.expression()(expression, scope_aliases={"source": "source"}) == (
        'F.to_timestamp_ntz(F.col("source.raw"), F.col("source.pattern"))'
    )


def test_v6_to_timestamp_ntz_without_format_renders_one_argument() -> None:
    @transform
    class ParseDefaultFormat(Transform):
        source = input(TimestampText)
        target = output(ParsedTimestamp)

        def convert(self, row: TimestampText) -> ParsedTimestamp:
            return ParsedTimestamp(value=to_timestamp_ntz(row.raw))

    recipe = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(ParseDefaultFormat, materialize_schemas=False).lowered,
    )
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"source": "source"}) == (
        'F.to_timestamp_ntz(F.col("source.raw"))'
    )


def test_v6_to_timestamp_ntz_online_evaluation_preserves_source_and_format_columns() -> None:
    expression = _expression()

    class Functions:
        @staticmethod
        def col(name):
            return name

        @staticmethod
        def to_timestamp_ntz(*args):
            return args

    assert EvaluatePySparkExpression().evaluate(
        expression,
        functions=Functions(),
        aliases={"source": "source"},
    ) == ("source.raw", "source.pattern")


def test_v6_to_timestamp_ntz_rejects_non_string_sources_and_formats() -> None:
    class InvalidSource(Schema):
        raw = integer(nullable=False)

    @transform
    class ParseInvalidSource(Transform):
        source = input(InvalidSource)
        target = output(ParsedTimestamp)

        def convert(self, row: InvalidSource) -> ParsedTimestamp:
            return ParsedTimestamp(value=to_timestamp_ntz(row.raw))

    class InvalidFormat(Schema):
        raw = string(nullable=False)
        pattern = integer(nullable=False)

    @transform
    class ParseInvalidFormat(Transform):
        source = input(InvalidFormat)
        target = output(ParsedTimestamp)

        def convert(self, row: InvalidFormat) -> ParsedTimestamp:
            return ParsedTimestamp(value=to_timestamp_ntz(row.raw, format=row.pattern))

    with pytest.raises(TypeError, match="to_timestamp_ntz"):
        Compiler.frontend.compile()(ParseInvalidSource, materialize_schemas=False)

    with pytest.raises(TypeError, match="to_timestamp_ntz"):
        Compiler.frontend.compile()(ParseInvalidFormat, materialize_schemas=False)
