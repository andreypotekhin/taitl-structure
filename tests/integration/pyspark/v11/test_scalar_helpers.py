from __future__ import annotations

import importlib
from datetime import date as date_value
from datetime import datetime
from typing import cast
from uuid import UUID

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
from structure.plugin.pyspark import chr, date, double, long, quote, random, string, timestamp, try_to_date, uuid

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        backend_name() not in {"pyspark41", "spark-connect41"},
        reason="V11 scalar helpers require the proven PySpark 4.1 profile",
    ),
]

SOURCE_MODULE = "integration.pyspark.v11.test_scalar_helpers"
PACKAGE = "integration_v11_scalar_helpers_generated"


class ScalarInput(Schema):
    id = long(nullable=False)
    code = long(nullable=True)
    text = string(nullable=True)
    raw_date = string(nullable=False)
    day = date(nullable=True)
    instant = timestamp(nullable=True)


class ScalarOutput(Schema):
    id = long(nullable=False)
    character = string(nullable=True)
    quoted = string(nullable=True)
    parsed = date(nullable=True)
    formatted = date(nullable=True)
    day = date(nullable=True)
    instant_day = date(nullable=True)
    literal_character = string(nullable=False)
    literal_quoted = string(nullable=True)


@transform
class ConvertScalars(Transform):
    rows = input(ScalarInput)
    converted = output(ScalarOutput)

    def convert(self, row: ScalarInput) -> ScalarOutput:
        return ScalarOutput(
            id=row.id,
            character=chr(row.code),
            quoted=quote(row.text),
            parsed=try_to_date(row.raw_date),
            formatted=try_to_date(row.raw_date, format="dd/MM/yyyy"),
            day=try_to_date(row.day),
            instant_day=try_to_date(row.instant),
            literal_character=chr(65),
            literal_quoted=quote("Don't"),
        )


class RandomInput(Schema):
    id = long(nullable=False)


class RandomOutput(Schema):
    id = long(nullable=False)
    sample = double(nullable=False)
    identifier = string(nullable=False)


@transform
class GenerateRandom(Transform):
    rows = input(RandomInput)
    generated = output(RandomOutput)

    def publish(self, row: RandomInput) -> RandomOutput:
        return RandomOutput(id=row.id, sample=random(seed=42), identifier=uuid(seed=42))


@transform
class GenerateUnseeded(Transform):
    rows = input(RandomInput)
    generated = output(RandomOutput)

    def publish(self, row: RandomInput) -> RandomOutput:
        return RandomOutput(id=row.id, sample=random(reproducible=False), identifier=uuid(reproducible=False))


def _files(transform, schemas):
    files = render_generated_project(
        transform,
        source_transform=f"{SOURCE_MODULE}.{transform.__name__}",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: schemas},
    )
    source = files[f"{PACKAGE}/pyspark/transforms/integration/pyspark/v11/test_scalar_helpers.py"]
    assert not any(token in source for token in ("SparkContext", "sparkContext", "_jdf", "_jvm", ".rdd"))
    return files, source


@pytest.mark.parametrize("ansi", ["true", "false"])
def test_scalar_helpers_match_native_online_and_generated(spark, tmp_path, ansi: str) -> None:
    from pyspark.sql import functions as F

    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    try:
        files, source_text = _files(ConvertScalars, [ScalarInput, ScalarOutput])
        for name in ("chr", "quote", "try_to_date"):
            assert f"F.{name}(" in source_text
        assert "'dd/MM/yyyy'" in source_text
        with generated_project(tmp_path, PACKAGE, files):
            schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_scalar_helpers")
            source = spark.createDataFrame(
                [
                    (1, 65, "Don't", "2024-02-29", date_value(2024, 2, 29), datetime(2024, 2, 29, 23, 59)),
                    (2, 321, "", "29/02/2024", None, None),
                    (3, -1, "back\\slash's", "2023-02-29", None, None),
                    (4, 0, "雪", "malformed", None, None),
                    (5, 256, None, "", None, None),
                    (6, None, "''", "0001-01-01", None, None),
                ],
                schemas.SCALAR_INPUT_SCHEMA,
            )
            online = ConvertScalars(rows=source).run(session(spark, execution_mode="online")).converted
            generated = (
                ConvertScalars(rows=source)
                .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
                .converted
            )
            native = source.select(
                "id",
                F.chr("code").alias("character"),
                F.quote("text").alias("quoted"),
                F.try_to_date("raw_date").alias("parsed"),
                F.try_to_date("raw_date", "dd/MM/yyyy").alias("formatted"),
                F.try_to_date("day").alias("day"),
                F.try_to_date("instant").alias("instant_day"),
                F.chr(F.lit(65)).alias("literal_character"),
                F.quote(F.lit("Don't")).alias("literal_quoted"),
            )
            assert_online_generated_parity(lambda: online, lambda: generated)
            assert rows(generated) == rows(native)
            assert generated.schema == online.schema == native.schema
            actual = rows(generated, "id")
            assert [row["character"] for row in actual] == ["A", "A", "", "\x00", "\x00", None]
            assert [row["quoted"] for row in actual] == [
                "'Don\\'t'",
                "''",
                "'back\\slash\\'s'",
                "'雪'",
                None,
                "'\\'\\''",
            ]
            assert [row["parsed"] for row in actual] == [
                date_value(2024, 2, 29),
                None,
                None,
                None,
                None,
                date_value(1, 1, 1),
            ]
            assert [row["formatted"] for row in actual] == [None, date_value(2024, 2, 29), None, None, None, None]
            assert actual[0]["day"] == actual[0]["instant_day"] == date_value(2024, 2, 29)
            assert not generated.schema["literal_character"].nullable
            assert generated.schema["literal_quoted"].nullable
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.parametrize("seeded", [True, False])
@pytest.mark.parametrize("partitions", [1, 2])
def test_random_helpers_match_their_seeded_or_nondeterministic_contract(
    spark, tmp_path, seeded: bool, partitions: int
) -> None:
    from pyspark.sql import functions as F

    transform = GenerateRandom if seeded else GenerateUnseeded
    files, source_text = _files(transform, [RandomInput, RandomOutput])
    for name in ("random", "uuid"):
        assert f"F.{name}(seed=42)" in source_text if seeded else f"F.{name}()" in source_text
    with generated_project(tmp_path, PACKAGE, files):
        source = spark.range(0, 10, numPartitions=partitions)
        online = transform(rows=source).run(session(spark, execution_mode="online")).generated
        generated = (
            transform(rows=source).run(session(spark, execution_mode="generated", generated_package=PACKAGE)).generated
        )
        assert generated.schema == online.schema
        if seeded:
            native = source.select("id", F.random(seed=42).alias("sample"), F.uuid(seed=42).alias("identifier"))
            assert_online_generated_parity(lambda: online, lambda: generated)
            assert rows(generated) == rows(native)
            assert generated.schema == native.schema
        for frame in (online, generated):
            actual = rows(frame)
            assert len(actual) == 10
            assert all(0 <= cast(float, row["sample"]) < 1 for row in actual)
            assert all(str(UUID(cast(str, row["identifier"]))) == row["identifier"] for row in actual)
            assert len({row["identifier"] for row in actual}) == 10
            assert not frame.schema["sample"].nullable
            assert not frame.schema["identifier"].nullable
