from datetime import time as time_value
from datetime import timezone

import pytest

from structure import *
from structure.plugin.api.v1.model import BackendCapabilityError, CapabilityRequirement
from structure.plugin.pyspark import *
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.types import LongType, StringType, TimeType
from structure.plugin.pyspark.dsl.types import time as time_type
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import RenderPySparkExpression
from structure.plugin.pyspark.schema.commands.RenderPySparkSchema import RenderPySparkSchema


@pytest.mark.parametrize("precision", range(7))
def test_time_precision_is_validated_and_participates_in_type_identity(precision: int) -> None:
    assert Time is TimeType
    assert time_type(precision) == TimeType(precision)
    assert TimeType(precision) == TimeType(precision)
    if precision < 6:
        assert TimeType(precision) != TimeType(precision + 1)


@pytest.mark.parametrize("precision", [-1, 7, True, 1.5, "3"])
def test_time_rejects_invalid_precision(precision) -> None:
    with pytest.raises(ValueError, match="integer from 0 through 6"):
        TimeType(precision)


def test_python_time_literal_preserves_wall_clock_fields_and_is_time_six() -> None:
    aware = time_value(23, 59, 59, 999999, tzinfo=timezone.utc)
    expression = literal(aware)
    assert expression.type == TimeType(6)
    assert expression.nullable is False
    assert expression.data is not None
    assert expression.data["value"] == aware


@pytest.mark.parametrize("variant", ["ordinary", "spark-connect"])
@pytest.mark.parametrize("profile", [">=4.1,<4.2"])
def test_time_helpers_have_typed_native_recipes(variant: str, profile: str) -> None:
    value = Expression(kind="field", type=TimeType(3), nullable=False, data={"field": "clock", "scope": "rows"})
    text = Expression(kind="field", type=StringType(), nullable=False, data={"field": "text", "scope": "rows"})
    unit = Expression(kind="field", type=StringType(), nullable=False, data={"field": "unit", "scope": "rows"})
    expressions = (
        (current_time(3), "F.current_time(3)", TimeType(3), False),
        (make_time(1, 2, 3.25), "F.make_time(F.lit(1), F.lit(2), F.lit(3.25))", TimeType(6), True),
        (to_time(text, format="HH:mm:ss"), "F.to_time(F.col(\"rows.text\"), F.lit('HH:mm:ss'))", TimeType(6), True),
        (try_to_time(text), "F.try_to_time(F.col(\"rows.text\"))", TimeType(6), True),
        (time_diff(unit, value, value), "F.time_diff(F.col(\"rows.unit\"), F.col(\"rows.clock\"), F.col(\"rows.clock\"))", LongType(), True),
        (time_trunc(unit, value), "F.time_trunc(F.col(\"rows.unit\"), F.col(\"rows.clock\"))", TimeType(3), True),
    )
    capabilities = PySparkCapabilities(target_profile=profile, target_variant=variant)
    for expression, expected, type, nullable in expressions:
        recipe = MapPySparkExpression().map(expression, capabilities=capabilities)
        assert RenderPySparkExpression()(recipe) == expected
        assert expression.type == type
        assert expression.nullable is nullable
        assert "_jvm" not in expected and ".rdd" not in expected


TIME_OPERATIONS = (
    (current_time(3), "current_time"),
    (make_time(1, 2, 3), "make_time"),
    (to_time("12:34:56"), "to_time"),
    (try_to_time("12:34:56"), "try_to_time"),
    (time_diff("hour", literal("12:34:56").cast(TimeType()), literal("12:34:56").cast(TimeType())), "time_diff"),
    (time_trunc("hour", literal("12:34:56").cast(TimeType())), "time_trunc"),
)


@pytest.mark.parametrize("expression,name", TIME_OPERATIONS)
@pytest.mark.parametrize("variant", ["ordinary", "spark-connect"])
@pytest.mark.parametrize("profile", [">=3.5,<4.1", ">=3.5,<4.0", ">=4.0,<4.1", ">=4.2,<4.3"])
def test_time_operations_reject_unproven_profiles(expression, name: str, profile: str, variant: str) -> None:
    with pytest.raises(BackendCapabilityError, match=f"expression.{name}") as error:
        MapPySparkExpression().map(expression, capabilities=PySparkCapabilities(target_profile=profile, target_variant=variant))
    assert f"{name}(...) requires PySpark >=4.1,<4.2" in error.value.decision.why


def test_time_schema_is_profile_gated_recursively_and_renders_precision() -> None:
    class Nested(Schema):
        clock = time(precision=3, nullable=False)

    class Record(Schema):
        clock = time(precision=6)
        clocks = array(time(precision=2), contains_null=False)

    capabilities = PySparkCapabilities(target_profile=">=4.1,<4.2")
    capabilities.require(CapabilityRequirement(group="schema", name="time"))
    assert "T.TimeType(6)" in RenderPySparkSchema()(Record)
    assert "T.ArrayType(T.TimeType(2), containsNull=False)" in RenderPySparkSchema()(Record)
    assert "T.TimeType(3)" in RenderPySparkSchema()(Nested)
    with pytest.raises(BackendCapabilityError, match="schema.time"):
        PySparkCapabilities().require(CapabilityRequirement(group="schema", name="time"))


def test_time_casts_preserve_precision_and_reject_unsupported_types() -> None:
    text = Expression(kind="field", type=StringType(), nullable=True, data={"field": "text", "scope": "rows"})
    clock = Expression(kind="field", type=TimeType(3), nullable=True, data={"field": "clock", "scope": "rows"})
    cast_text = text.cast(TimeType(2))
    cast_clock = clock.cast(TimeType(6))
    assert cast_text.data is not None and cast_text.data["spark_type"] == "time(2)"
    assert cast_clock.data is not None and cast_clock.data["spark_type"] == "time(6)"
    assert clock.cast(StringType()).type == StringType()
    with pytest.raises(TypeError, match="TIME casts only"):
        clock.cast(LongType())


def test_time_values_support_ordering_and_require_matching_precision_for_comparison() -> None:
    from structure.plugin.pyspark.dsl.types import BooleanType

    left = Expression(kind="field", type=TimeType(3), nullable=True, data={"field": "left", "scope": "rows"})
    right = Expression(kind="field", type=TimeType(3), nullable=False, data={"field": "right", "scope": "rows"})
    comparison = left < right
    assert comparison.type == BooleanType()
    assert comparison.nullable is True
    assert left.asc().type == TimeType(3)
    different_precision = Expression(
        kind="field", type=TimeType(6), nullable=False, data={"field": "different", "scope": "rows"}
    )
    incompatible = left < different_precision
    assert incompatible.data is not None
    assert incompatible.data["comparison_problem"] == (
        "Comparison requires compatible Structure expression types"
    )


def test_time_units_and_operands_are_checked_early() -> None:
    with pytest.raises(ValueError, match="unit"):
        time_diff("day", literal("12:00:00").cast(TimeType()), literal("12:00:00").cast(TimeType()))
    with pytest.raises(TypeError, match="TIME expression"):
        time_trunc("hour", literal("12:00:00"))
    with pytest.raises(TypeError, match="Integer or Long"):
        make_time(1.5, 2, 3)
