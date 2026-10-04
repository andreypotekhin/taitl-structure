from __future__ import annotations

import importlib
from decimal import Decimal
from typing import cast

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
from structure.plugin.pyspark import array, decimal, double, group_by, histogram_numeric, string, struct

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_histogram_numeric"
PACKAGE = "integration_v7_histogram_numeric_generated"


class HistogramInput(Schema):
    group = string(nullable=False)
    value = double(nullable=True)


class HistogramBucket(Schema):
    x = double(nullable=True)
    y = double(nullable=True)


class HistogramOutput(Schema):
    group = string(nullable=False)
    buckets = array(struct(HistogramBucket), contains_null=True, nullable=True)


class DecimalHistogramInput(Schema):
    group = string(nullable=False)
    value = decimal(8, 2, nullable=True)


class DecimalHistogramBucket(Schema):
    x = decimal(8, 2, nullable=True)
    y = double(nullable=True)


class DecimalHistogramOutput(Schema):
    group = string(nullable=False)
    buckets = array(struct(DecimalHistogramBucket), contains_null=True, nullable=True)


@transform
class NumericHistogram(Transform):
    rows = input(HistogramInput)
    result = output(HistogramOutput)

    def summarize(self, row: HistogramInput) -> HistogramOutput:
        group_by(row.group)
        return HistogramOutput(group=row.group, buckets=histogram_numeric(row.value, 3, as_=HistogramBucket))


@transform
class DecimalHistogram(Transform):
    rows = input(DecimalHistogramInput)
    result = output(DecimalHistogramOutput)

    def summarize(self, row: DecimalHistogramInput) -> DecimalHistogramOutput:
        group_by(row.group)
        return DecimalHistogramOutput(
            group=row.group,
            buckets=histogram_numeric(row.value, 3, as_=DecimalHistogramBucket),
        )


def test_histogram_numeric_matches_generated_execution_on_live_backend(spark, tmp_path) -> None:
    files = render_generated_project(
        NumericHistogram,
        source_transform=f"{SOURCE_MODULE}.NumericHistogram",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [HistogramInput, HistogramBucket, HistogramOutput]},
    )
    transform_path = f"{PACKAGE}/pyspark/transforms/integration/pyspark/v7/test_histogram_numeric.py"
    assert "histogram_numeric(" in files[transform_path]
    assert "HISTOGRAM_BUCKET_SCHEMA" in files[transform_path]

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_histogram_numeric")
        source = spark.createDataFrame(
            [("tenant-a", 1.0), ("tenant-a", 2.0), ("tenant-a", 3.0), ("tenant-a", None)],
            generated_schemas.HISTOGRAM_INPUT_SCHEMA,
        )
        online = NumericHistogram(rows=source).run(session(spark, execution_mode="online"))
        generated = NumericHistogram(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        actual = rows(generated.result, "group")

    assert len(actual) == 1
    buckets = actual[0]["buckets"]
    assert isinstance(buckets, list)
    assert 1 <= len(buckets) <= 3
    bucket_rows = cast(list[dict[str, object]], buckets)
    assert _bucket_total(bucket_rows) == pytest.approx(3.0)


def test_decimal_histogram_numeric_matches_generated_execution_on_pyspark_4(spark, tmp_path) -> None:
    if backend_name().endswith("35"):
        pytest.skip("Spark 3.5 HistogramNumeric throws ClassCastException for Decimal input")
    files = render_generated_project(
        DecimalHistogram,
        source_transform=f"{SOURCE_MODULE}.DecimalHistogram",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [DecimalHistogramInput, DecimalHistogramBucket, DecimalHistogramOutput]},
    )

    with generated_project(tmp_path, PACKAGE, files):
        generated_schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_histogram_numeric")
        source = spark.createDataFrame(
            [("tenant-a", Decimal("1.00")), ("tenant-a", Decimal("2.00"))],
            generated_schemas.DECIMAL_HISTOGRAM_INPUT_SCHEMA,
        )
        online = DecimalHistogram(rows=source).run(session(spark, execution_mode="online"))
        generated = DecimalHistogram(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        actual = rows(generated.result, "group")

    assert len(actual) == 1
    buckets = cast(list[dict[str, object]], actual[0]["buckets"])
    assert _bucket_total(buckets) == pytest.approx(2.0)


def _bucket_total(buckets: list[dict[str, object]]) -> float:
    values: list[float] = []
    for bucket in buckets:
        value = bucket["y"]
        assert isinstance(value, (int, float))
        values.append(float(value))
    return sum(values, 0.0)
