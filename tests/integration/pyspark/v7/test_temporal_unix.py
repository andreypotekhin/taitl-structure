from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import (
    date_part,
    from_unixtime,
    integer,
    long,
    make_timestamp,
    string,
    timestamp_ntz,
    to_timestamp,
    unix_timestamp,
)

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_temporal_unix"
PACKAGE = "integration_v7_temporal_unix_generated"


class EpochInput(Schema):
    id = string(nullable=False)
    seconds = long(nullable=True)


class EpochOutput(Schema):
    id = string(nullable=False)
    day = string(nullable=True)
    seconds_round_trip = long(nullable=True)
    month = integer(nullable=True)


@transform
class FormatEpoch(Transform):
    rows = input(EpochInput)
    formatted = output(EpochOutput)

    def publish(self, row: EpochInput) -> EpochOutput:
        formatted = from_unixtime(row.seconds, format="yyyy-MM-dd")
        parsed = to_timestamp(formatted, format="yyyy-MM-dd")
        return EpochOutput(
            id=row.id,
            day=formatted,
            seconds_round_trip=unix_timestamp(formatted, format="yyyy-MM-dd"),
            month=date_part("month", parsed),
        )


def test_v7_from_unixtime_matches_generated_execution_on_live_backend(spark, tmp_path) -> None:
    files = render_generated_project(
        FormatEpoch,
        source_transform=f"{SOURCE_MODULE}.FormatEpoch",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [EpochInput, EpochOutput]},
    )
    transform_path = f"{PACKAGE}/pyspark/transforms/integration/pyspark/v7/test_temporal_unix.py"
    assert "F.from_unixtime(" in files[transform_path]
    assert "F.unix_timestamp(" in files[transform_path]
    assert "F.date_part(F.lit('month')," in files[transform_path]

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_temporal_unix")
        source = spark.createDataFrame(
            [("row-1", 0), ("row-2", None)],
            generated_schemas.EPOCH_INPUT_SCHEMA,
        )

        online = FormatEpoch(rows=source).run(session(spark, execution_mode="online"))
        generated = FormatEpoch(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        actual = rows(generated.formatted, "id")

    assert actual == [
        {"id": "row-1", "day": "1970-01-01", "seconds_round_trip": 0, "month": 1},
        {"id": "row-2", "day": None, "seconds_round_trip": None, "month": None},
    ]


class ConfiguredTimestampInput(Schema):
    raw = string(nullable=False)
    pattern = string(nullable=False)
    year = integer(nullable=False)


class ConfiguredTimestampOutput(Schema):
    parsed = timestamp_ntz(nullable=False)
    built = timestamp_ntz(nullable=False)


@transform
class ConfiguredTimestamp(Transform):
    rows = input(ConfiguredTimestampInput)
    result = output(ConfiguredTimestampOutput)

    def publish(self, row: ConfiguredTimestampInput) -> ConfiguredTimestampOutput:
        parsed = to_timestamp(row.raw, format=row.pattern)
        built = make_timestamp(row.year, 10, 2, 9, 30, 0)
        return ConfiguredTimestampOutput(parsed=parsed, built=built)


def test_generic_timestamp_type_tracks_live_spark_configuration(spark, tmp_path) -> None:
    previous = spark.conf.get("spark.sql.timestampType")
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
    try:
        files = render_generated_project(
            ConfiguredTimestamp,
            source_transform=f"{SOURCE_MODULE}.ConfiguredTimestamp",
            generated_package=PACKAGE,
            source_schema_modules={SOURCE_MODULE: [ConfiguredTimestampInput, ConfiguredTimestampOutput]},
            spark_sql={"spark.sql.timestampType": "TIMESTAMP_NTZ"},
        )
        transform_path = f"{PACKAGE}/pyspark/transforms/integration/pyspark/v7/test_temporal_unix.py"
        assert "call_function" in files[transform_path]
        with generated_project(tmp_path, PACKAGE, files):
            generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_temporal_unix")
            source = spark.createDataFrame(
                [("2026-10-02", "yyyy-MM-dd", 2026)],
                generated_schemas.CONFIGURED_TIMESTAMP_INPUT_SCHEMA,
            )
            online = ConfiguredTimestamp(rows=source).run(session(spark, execution_mode="online"))
            generated = ConfiguredTimestamp(rows=source).run(
                session(spark, execution_mode="generated", generated_package=PACKAGE)
            )
            assert_online_generated_parity(lambda: online, lambda: generated)
            assert generated.result.schema["parsed"].dataType.typeName() == "timestamp_ntz"
            row = rows(generated.result, "parsed", "built")[0]
            assert str(row["parsed"]) == "2026-10-02 00:00:00"
            assert str(row["built"]) == "2026-10-02 09:30:00"
    finally:
        spark.conf.set("spark.sql.timestampType", previous)


def test_explicit_timestamp_parser_ansi_behavior_matches_spark(spark) -> None:
    from pyspark.sql import functions as F

    previous = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.set("spark.sql.ansi.enabled", "false")
        assert spark.range(1).select(F.to_timestamp_ntz(F.lit("not-a-timestamp"))).first()[0] is None
        assert spark.range(1).select(F.try_to_timestamp(F.lit("not-a-timestamp"))).first()[0] is None
        spark.conf.set("spark.sql.ansi.enabled", "true")
        with pytest.raises(Exception):
            spark.range(1).select(F.to_timestamp_ntz(F.lit("not-a-timestamp"))).first()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)
