from typing import cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.expressions import convert_timezone
from structure.plugin.pyspark.dsl.types import TimestampNTZType
from structure.plugin.pyspark.execution.logic.expressions.EvaluatePySparkExpression import EvaluatePySparkExpression
from structure.plugin.pyspark.schema.commands.MaterializePySparkSchema import MaterializePySparkSchema
from structure.plugin.pyspark.schema.commands.RenderPySparkSchema import RenderPySparkSchema


class TimeZoneInput(Schema):
    local_time = timestamp_ntz(nullable=True)
    source_zone = string(nullable=True)
    target_zone = string(nullable=False)


class ConvertedTime(Schema):
    local_time = timestamp_ntz(nullable=True)


class InstantInput(Schema):
    value = timestamp(nullable=False)


@transform
class ConvertTimeZone(Transform):
    source = input(TimeZoneInput)
    target = output(ConvertedTime)

    def convert(self, row: TimeZoneInput) -> ConvertedTime:
        return ConvertedTime(
            local_time=convert_timezone(row.source_zone, row.target_zone, row.local_time)
        )


@transform
class RejectInstantConversion(Transform):
    source = input(InstantInput)
    target = output(ConvertedTime)

    def convert(self, row: InstantInput) -> ConvertedTime:
        return ConvertedTime(local_time=convert_timezone("UTC", "Europe/Paris", row.value))


def test_v6_convert_timezone_accepts_typed_zone_strings_and_returns_ntz() -> None:
    lowered = cast(PySparkExecutionPlan, Compiler.frontend.compile()(ConvertTimeZone, materialize_schemas=False).lowered)
    projection = lowered.steps[0].projection[0].expression

    assert projection.type == TimestampNTZType()
    assert projection.nullable is True
    assert PySpark.render.expression()(projection, scope_aliases={"source": "source"}) == (
        'F.convert_timezone(F.col("source.source_zone"), '
        'F.col("source.target_zone"), F.col("source.local_time"))'
    )


def test_v6_convert_timezone_preserves_py_spark_default_source_zone_form() -> None:
    @transform
    class UseSessionZone(Transform):
        source = input(TimeZoneInput)
        target = output(ConvertedTime)

        def convert(self, row: TimeZoneInput) -> ConvertedTime:
            return ConvertedTime(local_time=convert_timezone(None, row.target_zone, row.local_time))

    lowered = cast(PySparkExecutionPlan, Compiler.frontend.compile()(UseSessionZone, materialize_schemas=False).lowered)
    expression = lowered.steps[0].projection[0].expression

    assert expression.nullable is True
    assert PySpark.render.expression()(expression, scope_aliases={"source": "source"}) == (
        'F.convert_timezone(None, F.col("source.target_zone"), F.col("source.local_time"))'
    )


def test_v6_convert_timezone_rejects_instant_timestamp_input() -> None:
    with pytest.raises(TypeError, match="TimestampNTZ"):
        Compiler.frontend.compile()(RejectInstantConversion, materialize_schemas=False)


def test_v6_convert_timezone_online_evaluation_preserves_typed_zone_columns() -> None:
    lowered = cast(PySparkExecutionPlan, Compiler.frontend.compile()(ConvertTimeZone, materialize_schemas=False).lowered)
    expression = lowered.steps[0].projection[0].expression

    class Functions:
        @staticmethod
        def col(name):
            return name

        @staticmethod
        def convert_timezone(source_zone, target_zone, timestamp):
            return source_zone, target_zone, timestamp

    result = EvaluatePySparkExpression().evaluate(
        expression,
        functions=Functions(),
        aliases={"source": "source"},
    )
    assert result == (
        "source.source_zone",
        "source.target_zone",
        "source.local_time",
    )


def test_v6_timestamp_ntz_materializes_and_renders_as_spark_timestamp_ntz() -> None:
    class Types:
        @staticmethod
        def TimestampNTZType():
            return "TimestampNTZType"

    ntz = TimestampNTZType()
    assert MaterializePySparkSchema().type(ntz, types=Types) == "TimestampNTZType"
    assert RenderPySparkSchema().type(ntz) == "T.TimestampNTZType()"
