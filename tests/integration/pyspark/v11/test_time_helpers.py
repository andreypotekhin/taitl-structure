from __future__ import annotations

import importlib
from datetime import time as time_value
from datetime import timezone

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import (
    TimeType,
    boolean,
    current_time,
    double,
    long,
    make_time,
    string,
    time,
    time_diff,
    time_trunc,
    to_time,
    try_to_time,
)
from structure.plugin.pyspark.dsl.types import StringType

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        backend_name() not in {"pyspark41", "spark-connect41"},
        reason="V11 TIME helpers require the proven PySpark 4.1 profile",
    ),
]

SOURCE_MODULE = "integration.pyspark.v11.test_time_helpers"
PACKAGE = "integration_v11_time_helpers_generated"


class TimeInput(Schema):
    id = long(nullable=False)
    start = time(nullable=False)
    end = time(precision=3, nullable=False)
    raw = string(nullable=True)
    pattern = string(nullable=False)
    invalid = string(nullable=True)
    unit = string(nullable=False)
    truncate_unit = string(nullable=False)
    hour = long(nullable=False)
    minute = long(nullable=False)
    second = double(nullable=False)


class TimeOutput(Schema):
    id = long(nullable=False)
    clock_a = time(precision=3, nullable=False)
    clock_b = time(precision=3, nullable=False)
    made = time(nullable=True)
    parsed = time(nullable=True)
    safe = time(nullable=True)
    diff = long(nullable=True)
    truncated = time(nullable=True)
    is_before = boolean(nullable=False)
    text = string(nullable=False)
    cast_time = time(precision=3, nullable=False)
    cast_text_time = time(precision=3, nullable=True)
    literal_clock = time(nullable=False)


@transform
class ConvertTimes(Transform):
    rows = input(TimeInput)
    converted = output(TimeOutput)

    def convert(self, row: TimeInput) -> TimeOutput:
        literal_clock = time_value(12, 34, 56, 123456, tzinfo=timezone.utc)
        return TimeOutput(
            id=row.id,
            clock_a=current_time(3),
            clock_b=current_time(3),
            made=make_time(row.hour, row.minute, row.second),
            parsed=to_time(row.raw, format=row.pattern),
            safe=try_to_time(row.invalid),
            diff=time_diff(row.unit, row.start, row.end),
            truncated=time_trunc(row.truncate_unit, row.start),
            is_before=row.start < time_value(12, 0),
            text=row.start.cast(StringType()),
            cast_time=row.start.cast(TimeType(precision=3)),
            cast_text_time=row.raw.cast(TimeType(precision=3)),
            literal_clock=literal_clock,
        )


def _files():
    files = render_generated_project(
        ConvertTimes,
        source_transform=f"{SOURCE_MODULE}.ConvertTimes",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [TimeInput, TimeOutput]},
    )
    source = files[f"{PACKAGE}/pyspark/transforms/integration/pyspark/v11/test_time_helpers.py"]
    generated = "\n".join(files.values())
    for spelling in ("F.current_time(3)", "F.make_time(", "F.to_time(", "F.try_to_time(", "F.time_diff(", "F.time_trunc("):
        assert spelling in generated
    assert "import datetime" in source
    assert not any(token in source for token in ("SparkContext", "sparkContext", "_jdf", "_jvm", ".rdd"))
    return files, source


def test_time_helpers_match_native_online_and_generated(spark, tmp_path) -> None:
    from pyspark.sql import functions as F

    previous = spark.conf.get("spark.sql.timeType.enabled", "false")
    spark.conf.set("spark.sql.timeType.enabled", "true")
    try:
        files, _ = _files()
        with generated_project(tmp_path, PACKAGE, files):
            schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_time_helpers")
            source = spark.createDataFrame(
                [
                    (1, time_value(23, 59, 59, 123456), time_value(0, 0, 1, 987000),
                     "01:02:03.123456", "HH:mm:ss.SSSSSS", "bad", "microsecond", "hour", 23, 59, 59.999999),
                    (2, time_value(1, 2, 3), time_value(1, 2, 4), None,
                     "HH:mm:ss", None, "minute", "minute", 1, 2, 3.0),
                ],
                schemas.TIME_INPUT_SCHEMA,
            )
            online = ConvertTimes(rows=source).run(session(spark, execution_mode="online")).converted
            generated = ConvertTimes(rows=source).run(
                session(spark, execution_mode="generated", generated_package=PACKAGE)
            ).converted
            native = source.select(
                "id",
                F.current_time(3).alias("clock_a"),
                F.current_time(3).alias("clock_b"),
                F.make_time("hour", "minute", "second").alias("made"),
                F.to_time("raw", "pattern").alias("parsed"),
                F.try_to_time("invalid").alias("safe"),
                F.time_diff("unit", "start", "end").alias("diff"),
                F.time_trunc("truncate_unit", "start").alias("truncated"),
                (F.col("start") < F.lit(time_value(12, 0))).alias("is_before"),
                F.col("start").cast("string").alias("text"),
                F.col("start").cast("time(3)").alias("cast_time"),
                F.col("raw").cast("time(3)").alias("cast_text_time"),
                F.lit(time_value(12, 34, 56, 123456, tzinfo=timezone.utc)).alias("literal_clock"),
            )
            assert_online_generated_parity(lambda: online.drop("clock_a", "clock_b"), lambda: generated.drop("clock_a", "clock_b"))
            assert online.schema == generated.schema == native.schema
            expected_schema = spark.createDataFrame([], schemas.TIME_OUTPUT_SCHEMA).schema
            assert online.schema == expected_schema
            for result in (online, generated, native):
                actual = {row["id"]: row for row in rows(result, "id")}
                assert all(row["clock_a"] == row["clock_b"] for row in actual.values())
                assert len({row["clock_a"] for row in actual.values()}) == 1
                assert actual[1]["made"] == time_value(23, 59, 59, 999999)
                assert actual[1]["parsed"] == time_value(1, 2, 3, 123456)
                assert actual[1]["safe"] is None
                assert actual[1]["diff"] == -(23 * 60 * 60 + 59 * 60 + 58) * 1_000_000 - 123456 + 987000
                assert actual[1]["truncated"] == time_value(23, 0)
                assert actual[1]["is_before"] is False
                assert actual[1]["cast_time"] == time_value(23, 59, 59, 123000)
                assert actual[1]["cast_text_time"] == time_value(1, 2, 3, 123456)
                assert actual[1]["literal_clock"] == time_value(12, 34, 56, 123456)
                assert actual[2]["parsed"] is None
                assert actual[2]["is_before"] is True
            with pytest.raises(Exception, match="CANNOT_PARSE_TIME"):
                spark.range(1).select(F.to_time(F.lit("malformed"))).collect()
    finally:
        spark.conf.set("spark.sql.timeType.enabled", previous)


@pytest.mark.parametrize("ansi", ["true", "false"])
def test_to_time_keeps_native_strict_parse_behavior_in_both_ansi_modes(spark, ansi: str) -> None:
    from pyspark.sql import functions as F

    previous_time = spark.conf.get("spark.sql.timeType.enabled", "false")
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.timeType.enabled", "true")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    try:
        with pytest.raises(Exception, match="CANNOT_PARSE_TIME"):
            spark.range(1).select(F.to_time(F.lit("malformed"))).collect()
        assert spark.range(1).select(F.try_to_time(F.lit("malformed"))).first()[0] is None
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)
        spark.conf.set("spark.sql.timeType.enabled", previous_time)


def test_disabled_time_feature_surfaces_sparks_native_error(spark) -> None:
    previous = spark.conf.get("spark.sql.timeType.enabled", "false")
    spark.conf.set("spark.sql.timeType.enabled", "false")
    try:
        with pytest.raises(Exception, match="(?i)time"):
            spark.sql("SELECT CAST('12:00:00' AS TIME)").collect()
    finally:
        spark.conf.set("spark.sql.timeType.enabled", previous)
