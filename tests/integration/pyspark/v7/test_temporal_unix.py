from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import date_part, from_unixtime, integer, long, string, to_timestamp, unix_timestamp

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
