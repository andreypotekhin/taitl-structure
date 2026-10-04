from datetime import date, datetime, timezone

import pytest

from structure.plugin.api.v1.model.CompilationSettings import compilation_settings
from structure.plugin.pyspark import (
    Interval,
    Temporal,
    date_diff,
    date_sub,
    dateadd,
    day,
    extract,
    interval,
    localtimestamp,
    make_interval,
    make_timestamp,
    make_timestamp_ltz,
    make_timestamp_ntz,
    timestamp_millis,
    timestamp_seconds,
    to_timestamp,
    to_timestamp_ltz,
    to_unix_timestamp,
    try_to_timestamp,
    types,
    unix_seconds,
    unix_timestamp,
)
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.types import (
    DateType,
    DecimalType,
    IntegerType,
    IntervalType,
    LongType,
    TimestampNTZType,
    TimestampType,
)
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import RenderPySparkExpression


class _Capabilities:
    def require(self, requirement):
        return None


def _render(expression):
    return RenderPySparkExpression()(MapPySparkExpression().map(expression, capabilities=_Capabilities()))


def test_temporal_constants_are_strings_without_duplicate_doy_member():
    assert Temporal.DAY_OF_YEAR == "doy"
    assert Interval.YEAR_TO_MONTH == "year_to_month"
    assert Interval.DAY == "day"
    assert "DOY" not in Temporal.__members__


def test_temporal_aliases_preserve_pyspark_spelling_and_types():
    date_value = literal(date(2026, 10, 2))
    assert isinstance(date_diff(date_value, date_value).type, IntegerType)
    assert isinstance(dateadd(date_value, days=1).type, DateType)
    assert isinstance(day(date_value).type, IntegerType)
    assert _render(date_diff(date_value, date_value)).startswith("F.date_diff(")
    assert _render(dateadd(date_value, days=1)).startswith("F.dateadd(")
    assert _render(day(date_value)).startswith("F.day(")


def test_date_sub_accepts_a_typed_row_dependent_day_count():
    previous = date_sub(literal(date(2026, 10, 2)), days=literal(2))
    assert isinstance(previous.type, DateType)
    assert _render(previous).startswith("F.date_sub(")
    assert "F.lit(2)" in _render(previous)


def test_localtimestamp_is_ntz_query_clock():
    expression = localtimestamp()
    assert isinstance(expression.type, TimestampNTZType)
    assert expression.data is not None and expression.data["query_stable"] is True


def test_explicit_timestamp_constructors_and_parsers_have_fixed_types():
    components = (literal(2026), literal(10), literal(2), literal(9), literal(30), literal(1.25))
    ltz = make_timestamp_ltz(*components, timezone=literal("UTC"))
    ntz = make_timestamp_ntz(*components)
    assert isinstance(ltz.type, TimestampType)
    assert isinstance(ntz.type, TimestampNTZType)
    assert _render(ltz).startswith("F.make_timestamp_ltz(")
    assert _render(ntz).startswith("F.make_timestamp_ntz(")
    parsed = to_timestamp_ltz(literal("2026-10-02"), format=literal("yyyy-MM-dd"))
    assert isinstance(parsed.type, TimestampType)
    assert _render(parsed).startswith("F.to_timestamp_ltz(")


def test_epoch_helpers_and_unix_parser_keep_distinct_contracts():
    assert isinstance(timestamp_seconds(literal(1.25)).type, TimestampType)
    assert isinstance(timestamp_millis(literal(1250)).type, TimestampType)
    assert isinstance(unix_seconds(literal(datetime(2026, 10, 2, tzinfo=timezone.utc))).type, LongType)
    assert _render(timestamp_seconds(literal(1.25))).startswith("F.timestamp_seconds(")
    assert _render(to_unix_timestamp(literal("2026"), format=literal("yyyy"))).startswith("F.to_unix_timestamp(")
    assert _render(unix_timestamp(literal("2026"), format=literal("yyyy"))).startswith("F.call_function('unix_timestamp',")
    query_clock = unix_timestamp()
    assert query_clock.data is not None and query_clock.data["query_stable"] is True


def test_generic_timestamp_type_and_warning_follow_compilation_settings():
    components = (2026, 10, 2, 9, 30, 1)
    with compilation_settings({"spark.sql.timestampType": "TIMESTAMP_NTZ"}):
        created = make_timestamp(*components, timezone="UTC")
        parsed = to_timestamp("2026-10-02")
        tried = try_to_timestamp("invalid")
    assert all(isinstance(item.type, TimestampNTZType) for item in (created, parsed, tried))
    assert created.data is not None and created.data["warnings"] == ("PYSPARK-W2706",)
    assert isinstance(make_timestamp(*components).type, TimestampType)
    assert _render(to_timestamp(literal("2026"), format=literal("yyyy"))).startswith("F.call_function('to_timestamp',")


def test_interval_qualifiers_components_and_extract_aliases():
    year_month = interval(type=Interval.YEAR_TO_MONTH, years=2, months=3)
    day = interval(unit=Interval.DAY, days=4)
    assert year_month.type == types.interval(type=Interval.YEAR_TO_MONTH)
    assert day.type == types.interval(unit=Interval.DAY)
    assert isinstance(make_interval(years=1, days=2).type, IntervalType)
    assert _render(year_month).endswith(".cast('interval year to month')")
    assert _render(day).endswith(".cast('interval day')")
    assert isinstance(extract(Temporal.DAY_OF_YEAR, literal(date(2026, 10, 2))).type, IntegerType)
    alias = extract("dayofyear", literal(date(2026, 10, 2)))
    assert alias.data is not None and alias.data["field"] == "doy"
    assert isinstance(extract("seconds", day).type, DecimalType)
    with pytest.raises(TypeError, match="requires exactly"):
        interval(type=Interval.YEAR_TO_MONTH, years=2)


def test_interval_subtraction_type_tracks_legacy_setting():
    left = literal(date(2026, 10, 2))
    right = literal(date(2026, 10, 1))
    assert (left - right).type == types.interval(type=Interval.DAY_TO_SECOND)
    with compilation_settings({"spark.sql.legacy.interval.enabled": True}):
        assert (left - right).type == types.interval(type=Interval.CALENDAR)


def test_calendar_interval_date_addition_preserves_spark_date_type():
    value = literal(date(2026, 10, 2))
    assert isinstance((value + make_interval(months=1, days=1)).type, DateType)
