from __future__ import annotations

import importlib
from datetime import datetime

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import (
    aes_decrypt,
    aes_encrypt,
    binary,
    bitmap_construct_agg,
    bitmap_or_agg,
    bitwise_not,
    ceiling,
    convert_timezone,
    curdate,
    current_date,
    current_timestamp,
    current_timezone,
    date,
    double,
    from_utc_timestamp,
    group_by,
    hll_sketch_agg,
    hll_union_agg,
    integer,
    localtimestamp,
    negate,
    negative,
    now,
    positive,
    power,
    repartition_by_range,
    stack,
    string,
    timestamp,
    timestamp_ntz,
    to_utc_timestamp,
)
from structure.plugin.pyspark.dsl.field import bitmap as bitmap_field
from structure.plugin.pyspark.dsl.field import hll_sketch as hll_sketch_field

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_runtime_matrix_closeout"
PACKAGE = "integration_v7_runtime_matrix_closeout"


class ClockCryptoInput(Schema):
    payload = string(nullable=False)
    key = string(nullable=False)


class ClockCryptoOutput(Schema):
    current_at = timestamp(nullable=False)
    now_at = timestamp(nullable=False)
    local_at = timestamp_ntz(nullable=False)
    today = date(nullable=False)
    curdate_value = date(nullable=False)
    timezone = string(nullable=False)
    decrypted = binary(nullable=True)


@transform
class QueryClocksAndCrypto(Transform):
    rows = input(ClockCryptoInput)
    result = output(ClockCryptoOutput)

    def publish(self, row: ClockCryptoInput) -> ClockCryptoOutput:
        ciphertext = aes_encrypt(row.payload, key=row.key)
        return ClockCryptoOutput(
            current_at=current_timestamp(),
            now_at=now(),
            local_at=localtimestamp(),
            today=current_date(),
            curdate_value=curdate(),
            timezone=current_timezone(),
            decrypted=aes_decrypt(ciphertext, key=row.key),
        )


class SketchInput(Schema):
    group = string(nullable=False)
    merge_group = string(nullable=False)
    value = integer(nullable=False)


class SketchState(Schema):
    group = string(nullable=False)
    merge_group = string(nullable=False)
    hll = hll_sketch_field(lg_config_k=12, nullable=True)
    bitmap = bitmap_field(nullable=True)


class MergedSketch(Schema):
    group = string(nullable=False)
    hll = hll_sketch_field(lg_config_k=12, nullable=True)
    bitmap = bitmap_field(nullable=True)


@transform
class BuildSketchStates(Transform):
    rows = input(SketchInput)
    result = output(SketchState)

    def summarize(self, row: SketchInput) -> SketchState:
        group_by(row.group, row.merge_group)
        return SketchState(
            group=row.group,
            merge_group=row.merge_group,
            hll=hll_sketch_agg(row.value, lg_config_k=12),
            bitmap=bitmap_construct_agg(row.value),
        )


@transform
class MergeSketchStates(Transform):
    rows = input(SketchState)
    result = output(MergedSketch)

    def summarize(self, row: SketchState) -> MergedSketch:
        group_by(row.merge_group)
        return MergedSketch(
            group=row.merge_group,
            hll=hll_union_agg(row.hll),
            bitmap=bitmap_or_agg(row.bitmap),
        )


class StackRangeInput(Schema):
    item_id = integer(nullable=False)
    label = string(nullable=False)
    score = integer(nullable=False)


class StackOutput(Schema):
    item_id = integer(nullable=False, alias="stacked_id")
    label = string(nullable=True, alias="stacked_label")


class StackRangeOutput(Schema):
    item_id = integer(nullable=False)
    label = string(nullable=False)
    score = integer(nullable=False)


class TimeZoneInput(Schema):
    instant = timestamp(nullable=False)
    wall_time = timestamp_ntz(nullable=False)


class TimeZoneOutput(Schema):
    utc = timestamp(nullable=False)
    local = timestamp(nullable=False)
    converted = timestamp_ntz(nullable=False)


class BitwiseInput(Schema):
    value = integer(nullable=False)


class BitwiseOutput(Schema):
    inverted = integer(nullable=False)


class NumericAliasInput(Schema):
    value = double(nullable=True)
    exponent = double(nullable=False)


class NumericAliasOutput(Schema):
    ceiling_value = double(nullable=True)
    negate_value = double(nullable=True)
    negative_value = double(nullable=True)
    positive_value = double(nullable=True)
    power_value = double(nullable=True)


@transform
class StackRows(Transform):
    rows = input(StackRangeInput)
    result = output(StackOutput)

    def expand(self, row: StackRangeInput) -> StackOutput:
        return stack(2, row.item_id, row.label, row.item_id + 1, to=StackOutput)


@transform
class RangePartitionRows(Transform):
    rows = input(StackRangeInput)
    result = output(StackRangeOutput)

    def partition(self, row: StackRangeInput) -> StackRangeOutput:
        return StackRangeOutput.project(repartition_by_range(2, row.score.asc(), row.item_id))


@transform
class ConvertTimeZones(Transform):
    rows = input(TimeZoneInput)
    result = output(TimeZoneOutput)

    def convert(self, row: TimeZoneInput) -> TimeZoneOutput:
        return TimeZoneOutput(
            utc=to_utc_timestamp(row.instant, timezone="America/Los_Angeles"),
            local=from_utc_timestamp(row.instant, timezone="America/Los_Angeles"),
            converted=convert_timezone("America/Los_Angeles", "Asia/Tokyo", row.wall_time),
        )


@transform
class InvertBitwiseValue(Transform):
    rows = input(BitwiseInput)
    result = output(BitwiseOutput)

    def invert(self, row: BitwiseInput) -> BitwiseOutput:
        return BitwiseOutput(inverted=bitwise_not(row.value))


@transform
class NumericAliases(Transform):
    rows = input(NumericAliasInput)
    result = output(NumericAliasOutput)

    def evaluate(self, row: NumericAliasInput) -> NumericAliasOutput:
        return NumericAliasOutput(
            ceiling_value=ceiling(row.value),
            negate_value=negate(row.value),
            negative_value=negative(row.value),
            positive_value=positive(row.value),
            power_value=power(row.value, row.exponent),
        )


def test_query_clocks_and_aes_gcm_execute_online_and_generated(spark, tmp_path) -> None:
    files = render_generated_project(
        QueryClocksAndCrypto,
        source_transform=f"{SOURCE_MODULE}.QueryClocksAndCrypto",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [ClockCryptoInput, ClockCryptoOutput]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_runtime_matrix_closeout")
        source = spark.createDataFrame(
            [("payload survives AES-GCM", "Sixteen byte key")], schemas.CLOCK_CRYPTO_INPUT_SCHEMA
        )
        online = QueryClocksAndCrypto(rows=source).run(session(spark, execution_mode="online"))
        generated = QueryClocksAndCrypto(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        for result in (online.result, generated.result):
            actual = rows(result)[0]
            assert actual["current_at"] == actual["now_at"]
            assert actual["today"] == actual["curdate_value"]
            assert actual["timezone"] == "UTC"
            assert actual["decrypted"] == b"payload survives AES-GCM"
            assert actual["local_at"] is not None


def _measure_sketches(frame) -> tuple[int, int]:
    from pyspark.sql import functions as spark_functions

    measured = frame.select(
        spark_functions.hll_sketch_estimate("hll").alias("hll_count"),
        spark_functions.bitmap_count("bitmap").alias("bitmap_count"),
    ).first()
    assert measured is not None
    return int(measured["hll_count"]), int(measured["bitmap_count"])


def test_hll_and_bitmap_aggregates_execute_online_and_generated(spark, tmp_path) -> None:
    state_files = render_generated_project(
        BuildSketchStates,
        source_transform=f"{SOURCE_MODULE}.BuildSketchStates",
        generated_package=f"{PACKAGE}_states",
        source_schema_modules={SOURCE_MODULE: [SketchInput, SketchState]},
    )
    merge_files = render_generated_project(
        MergeSketchStates,
        source_transform=f"{SOURCE_MODULE}.MergeSketchStates",
        generated_package=f"{PACKAGE}_merge",
        source_schema_modules={SOURCE_MODULE: [SketchState, MergedSketch]},
    )
    state_package, merge_package = f"{PACKAGE}_states", f"{PACKAGE}_merge"

    with generated_project(tmp_path / "states", state_package, state_files):
        schemas = importlib.import_module(f"{state_package}.pyspark.schemas.test_runtime_matrix_closeout")
        source = spark.createDataFrame(
            [("a", "all", 1), ("a", "all", 2), ("b", "all", 3), ("b", "all", 4)],
            schemas.SKETCH_INPUT_SCHEMA,
        )
        online_states = BuildSketchStates(rows=source).run(session(spark, execution_mode="online"))
        generated_states = BuildSketchStates(rows=source).run(
            session(spark, execution_mode="generated", generated_package=state_package)
        )

        with generated_project(tmp_path / "merge", merge_package, merge_files):
            online_merged = MergeSketchStates(rows=online_states.result).run(
                session(spark, execution_mode="online")
            )
            generated_merged = MergeSketchStates(rows=generated_states.result).run(
                session(spark, execution_mode="generated", generated_package=merge_package)
            )
            assert _measure_sketches(online_merged.result) == (4, 4)
            assert _measure_sketches(generated_merged.result) == (4, 4)


def test_stack_and_range_repartition_preserve_live_results(spark, tmp_path) -> None:
    stack_files = render_generated_project(
        StackRows,
        source_transform=f"{SOURCE_MODULE}.StackRows",
        generated_package=f"{PACKAGE}_stack",
        source_schema_modules={SOURCE_MODULE: [StackRangeInput, StackOutput]},
    )
    range_files = render_generated_project(
        RangePartitionRows,
        source_transform=f"{SOURCE_MODULE}.RangePartitionRows",
        generated_package=f"{PACKAGE}_range",
        source_schema_modules={SOURCE_MODULE: [StackRangeInput, StackRangeOutput]},
    )
    stack_package, range_package = f"{PACKAGE}_stack", f"{PACKAGE}_range"

    with generated_project(tmp_path / "stack", stack_package, stack_files):
        schemas = importlib.import_module(f"{stack_package}.pyspark.schemas.test_runtime_matrix_closeout")
        source = spark.createDataFrame(
            [(7, "cat", 4)], schemas.STACK_RANGE_INPUT_SCHEMA
        )
        stack_online = StackRows(rows=source).run(session(spark, execution_mode="online"))
        stack_generated = StackRows(rows=source).run(
            session(spark, execution_mode="generated", generated_package=stack_package)
        )
        assert_online_generated_parity(lambda: stack_online, lambda: stack_generated)
        assert rows(stack_generated.result, "stacked_id") == [
            {"stacked_id": 7, "stacked_label": "cat"},
            {"stacked_id": 8, "stacked_label": None},
        ]

    with generated_project(tmp_path / "range", range_package, range_files):
        range_online = RangePartitionRows(rows=source).run(session(spark, execution_mode="online"))
        range_generated = RangePartitionRows(rows=source).run(
            session(spark, execution_mode="generated", generated_package=range_package)
        )
        assert_online_generated_parity(lambda: range_online, lambda: range_generated)
        assert rows(range_generated.result, "item_id") == [{"item_id": 7, "label": "cat", "score": 4}]


def test_sql_bitwise_not_matches_native_and_generated_execution(spark, tmp_path) -> None:
    package = f"{PACKAGE}_bitwise_not"
    files = render_generated_project(
        InvertBitwiseValue,
        source_transform=f"{SOURCE_MODULE}.InvertBitwiseValue",
        generated_package=package,
        source_schema_modules={SOURCE_MODULE: [BitwiseInput, BitwiseOutput]},
    )
    with generated_project(tmp_path, package, files):
        schemas = importlib.import_module(f"{package}.pyspark.schemas.test_runtime_matrix_closeout")
        source = spark.createDataFrame([(-6,), (0,), (5,)], schemas.BITWISE_INPUT_SCHEMA)
        online = InvertBitwiseValue(rows=source).run(session(spark, execution_mode="online"))
        generated = InvertBitwiseValue(rows=source).run(
            session(spark, execution_mode="generated", generated_package=package)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        assert rows(generated.result, "inverted") == [
            {"inverted": -6},
            {"inverted": -1},
            {"inverted": 5},
        ]


def test_numeric_aliases_match_native_and_generated_execution(spark, tmp_path) -> None:
    package = f"{PACKAGE}_numeric_aliases"
    files = render_generated_project(
        NumericAliases,
        source_transform=f"{SOURCE_MODULE}.NumericAliases",
        generated_package=package,
        source_schema_modules={SOURCE_MODULE: [NumericAliasInput, NumericAliasOutput]},
    )
    with generated_project(tmp_path, package, files):
        schemas = importlib.import_module(f"{package}.pyspark.schemas.test_runtime_matrix_closeout")
        source = spark.createDataFrame([(-2.3, 2.0), (2.3, 3.0), (None, 2.0)], schemas.NUMERIC_ALIAS_INPUT_SCHEMA)
        online = NumericAliases(rows=source).run(session(spark, execution_mode="online"))
        generated = NumericAliases(rows=source).run(
            session(spark, execution_mode="generated", generated_package=package)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)
        actual = rows(generated.result, "ceiling_value")
        assert actual[0] == {
            "ceiling_value": None,
            "negate_value": None,
            "negative_value": None,
            "positive_value": None,
            "power_value": None,
        }
        for row, expected in zip(
            actual[1:],
            [
                (-2.0, 2.3, 2.3, -2.3, 5.29),
                (3.0, -2.3, -2.3, 2.3, 12.167),
            ],
        ):
            for name, value in zip(
                ("ceiling_value", "negate_value", "negative_value", "positive_value", "power_value"),
                expected,
            ):
                assert row[name] == pytest.approx(value)


def test_timezone_conversions_match_native_and_generated_execution(spark, tmp_path) -> None:
    from pyspark.sql import functions as spark_functions

    files = render_generated_project(
        ConvertTimeZones,
        source_transform=f"{SOURCE_MODULE}.ConvertTimeZones",
        generated_package=f"{PACKAGE}_timezones",
        source_schema_modules={SOURCE_MODULE: [TimeZoneInput, TimeZoneOutput]},
    )
    package = f"{PACKAGE}_timezones"
    with generated_project(tmp_path, package, files):
        schemas = importlib.import_module(f"{package}.pyspark.schemas.test_runtime_matrix_closeout")
        source = spark.createDataFrame(
            [(datetime(2026, 1, 15, 12), datetime(2026, 1, 15, 12))],
            schemas.TIME_ZONE_INPUT_SCHEMA,
        )
        online = ConvertTimeZones(rows=source).run(session(spark, execution_mode="online"))
        generated = ConvertTimeZones(rows=source).run(
            session(spark, execution_mode="generated", generated_package=package)
        )
        native = source.select(
            spark_functions.to_utc_timestamp("instant", "America/Los_Angeles").alias("utc"),
            spark_functions.from_utc_timestamp("instant", "America/Los_Angeles").alias("local"),
            spark_functions.convert_timezone(
                spark_functions.lit("America/Los_Angeles"),
                spark_functions.lit("Asia/Tokyo"),
                "wall_time",
            ).alias("converted"),
        )
        assert_online_generated_parity(lambda: online.result, lambda: generated.result)
        expected = rows(native, "utc")
        assert rows(online.result, "utc") == expected
        assert rows(generated.result, "utc") == expected
