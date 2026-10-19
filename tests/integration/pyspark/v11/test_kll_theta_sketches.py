from __future__ import annotations

import builtins
import shutil
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_projects,
    session,
)

from structure import Schema, Transform, input, output, transform
from structure.plugin.pyspark import (
    TimeWindow,
    array,
    double,
    float,
    group_by,
    kll_sketch,
    kll_sketch_agg,
    kll_sketch_get_n,
    kll_sketch_get_quantile,
    kll_sketch_get_rank,
    kll_sketch_merge,
    kll_sketch_to_string,
    long,
    string,
    theta_difference,
    theta_intersection,
    theta_intersection_agg,
    theta_sketch,
    theta_sketch_agg,
    theta_sketch_estimate,
    theta_union,
    theta_union_agg,
    timestamp,
    watermark,
    window,
)
from structure.plugin.pyspark.dsl.field import array as array_field
from structure.plugin.pyspark.dsl.field import struct

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        backend_name() not in {"pyspark41", "spark-connect41"},
        reason="KLL and Theta require exact PySpark 4.1 profiles",
    ),
]

SOURCE_MODULE = "integration.pyspark.v11.test_kll_theta_sketches"
PACKAGE = "integration_v11_kll_theta_generated"


class RawValues(Schema):
    group = string(nullable=False)
    value = double(nullable=True)
    long_value = long(nullable=False)
    float_value = float(nullable=False)
    member = string(nullable=True)
    other_member = string(nullable=True)


class SketchState(Schema):
    group = string(nullable=False)
    latency = kll_sketch(double(), nullable=False)
    mixed_latency = kll_sketch(double(), nullable=False)
    long_latency = kll_sketch(long(), nullable=False)
    float_latency = kll_sketch(float(), nullable=False)
    members = theta_sketch(nullable=False)
    other_members = theta_sketch(nullable=False)
    filtered_latency = kll_sketch(double(), nullable=False)
    filtered_members = theta_sketch(nullable=False)


class SketchMetrics(Schema):
    group = string(nullable=False)
    count = long(nullable=False)
    median = double(nullable=False)
    rank = double(nullable=False)
    quantiles = array_field(double(), contains_null=False, nullable=False)
    ranks = array_field(double(), contains_null=False, nullable=False)
    rendering = string(nullable=False)
    long_median = long(nullable=False)
    float_median = float(nullable=False)
    merged_count = long(nullable=False)
    distinct_members = long(nullable=False)
    union_members = long(nullable=False)
    intersection_members = long(nullable=False)
    difference_members = long(nullable=False)
    filtered_count = long(nullable=False)
    filtered_members = long(nullable=False)


class ThetaAggregateState(Schema):
    group = string(nullable=False)
    union_sketch = theta_sketch(nullable=False)
    intersection_sketch = theta_sketch(nullable=False)


class ThetaAggregateMetrics(Schema):
    group = string(nullable=False)
    union_members = long(nullable=False)
    intersection_members = long(nullable=False)


class EmptyKllInput(Schema):
    group = string(nullable=False)
    value = double(nullable=True)


class EmptyKllState(Schema):
    group = string(nullable=False)
    sketch = kll_sketch(double(), nullable=False)


class EmptyKllMetrics(Schema):
    group = string(nullable=False)
    count = long(nullable=False)
    median = double(nullable=False)


class StreamingValues(Schema):
    group = string(nullable=False)
    event_time = timestamp(nullable=False)
    value = double(nullable=False)
    member = string(nullable=False)


class StreamingSketchState(Schema):
    bucket = struct(TimeWindow, nullable=False)
    latency = kll_sketch(double(), nullable=False)
    members = theta_sketch(nullable=False)


@transform
class SummarizeValues(Transform):
    rows = input(RawValues)
    result = output(SketchState)

    def summarize(self, row: RawValues) -> SketchState:
        group_by(row.group)
        return SketchState(
            group=row.group,
            latency=kll_sketch_agg(row.value, k=64),
            mixed_latency=kll_sketch_agg(row.value, k=32),
            long_latency=kll_sketch_agg(row.long_value, k=64),
            float_latency=kll_sketch_agg(row.float_value, k=64),
            members=theta_sketch_agg(row.member, lg_nom_entries=12),
            other_members=theta_sketch_agg(row.other_member, lg_nom_entries=12),
            filtered_latency=kll_sketch_agg(row.value, k=64, where=row.value > 1),
            filtered_members=theta_sketch_agg(row.member, where=row.member == "x"),
        )


@transform
class ReadSketches(Transform):
    sketches = input(SketchState)
    result = output(SketchMetrics)

    def inspect(self, row: SketchState) -> SketchMetrics:
        return SketchMetrics(
            group=row.group,
            count=kll_sketch_get_n(row.latency),
            median=kll_sketch_get_quantile(row.latency, 0.5),
            rank=kll_sketch_get_rank(row.latency, 2.0),
            quantiles=kll_sketch_get_quantile(row.latency, array(0.25, 0.5, 0.75)),
            ranks=kll_sketch_get_rank(row.latency, array(1.0, 2.0, 3.0)),
            rendering=kll_sketch_to_string(row.latency),
            long_median=kll_sketch_get_quantile(row.long_latency, 0.5),
            float_median=kll_sketch_get_quantile(row.float_latency, 0.5),
            merged_count=kll_sketch_get_n(kll_sketch_merge(row.latency, row.mixed_latency)),
            distinct_members=theta_sketch_estimate(row.members),
            union_members=theta_sketch_estimate(theta_union(row.members, row.other_members)),
            intersection_members=theta_sketch_estimate(theta_intersection(row.members, row.other_members)),
            difference_members=theta_sketch_estimate(theta_difference(row.members, row.other_members)),
            filtered_count=kll_sketch_get_n(row.filtered_latency),
            filtered_members=theta_sketch_estimate(row.filtered_members),
        )


@transform
class CombineThetaSketches(Transform):
    sketches = input(SketchState)
    result = output(ThetaAggregateState)

    def combine(self, row: SketchState) -> ThetaAggregateState:
        group_by(row.group)
        return ThetaAggregateState(
            group=row.group,
            union_sketch=theta_union_agg(row.members),
            intersection_sketch=theta_intersection_agg(row.members),
        )


@transform
class ReadThetaAggregates(Transform):
    sketches = input(ThetaAggregateState)
    result = output(ThetaAggregateMetrics)

    def inspect(self, row: ThetaAggregateState) -> ThetaAggregateMetrics:
        return ThetaAggregateMetrics(
            group=row.group,
            union_members=theta_sketch_estimate(row.union_sketch),
            intersection_members=theta_sketch_estimate(row.intersection_sketch),
        )


@transform
class SummarizeEmptyKll(Transform):
    rows = input(EmptyKllInput)
    result = output(EmptyKllState)

    def summarize(self, row: EmptyKllInput) -> EmptyKllState:
        group_by(row.group)
        return EmptyKllState(group=row.group, sketch=kll_sketch_agg(row.value))


@transform
class ReadEmptyKll(Transform):
    sketches = input(EmptyKllState)
    result = output(EmptyKllMetrics)

    def inspect(self, row: EmptyKllState) -> EmptyKllMetrics:
        return EmptyKllMetrics(
            group=row.group,
            count=kll_sketch_get_n(row.sketch),
            median=kll_sketch_get_quantile(row.sketch, 0.5),
        )


@transform(streaming=True)
class SummarizeStreamingSketches(Transform):
    rows = input(StreamingValues, streaming=True)
    result = output(StreamingSketchState)

    def summarize(self, row: StreamingValues) -> StreamingSketchState:
        watermark(row.event_time, delay="1 minute")
        bucket = window(row.event_time, "10 minutes")
        group_by(bucket=bucket)
        return StreamingSketchState(
            bucket=bucket,
            latency=kll_sketch_agg(row.value, k=64),
            members=theta_sketch_agg(row.member, lg_nom_entries=12),
        )


def test_kll_theta_values_and_metadata_match_native_online_and_generated(spark, tmp_path):
    from pyspark.sql import functions as F

    files = render_generated_projects(
        [
            (SummarizeValues, f"{SOURCE_MODULE}.SummarizeValues"),
            (ReadSketches, f"{SOURCE_MODULE}.ReadSketches"),
            (CombineThetaSketches, f"{SOURCE_MODULE}.CombineThetaSketches"),
            (ReadThetaAggregates, f"{SOURCE_MODULE}.ReadThetaAggregates"),
        ],
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [RawValues, SketchState, SketchMetrics, ThetaAggregateState, ThetaAggregateMetrics]},
    )
    producer = files[f"{PACKAGE}/pyspark/transforms/integration/pyspark/v11/test_kll_theta_sketches.py"]
    reader = producer
    schemas = files[f"{PACKAGE}/pyspark/schemas/test_kll_theta_sketches.py"]
    assert "F.kll_sketch_agg_double(" in producer
    assert "F.theta_sketch_agg(" in producer
    assert "F.kll_sketch_agg_bigint(" in producer
    assert "F.kll_sketch_agg_float(" in producer
    assert "F.kll_sketch_get_n_double(" in reader
    assert "F.kll_sketch_get_quantile_double(" in reader
    assert "F.kll_sketch_get_rank_double(" in reader
    assert "F.kll_sketch_merge_double(" in reader
    assert "F.kll_sketch_to_string_double(" in reader
    assert "F.theta_sketch_estimate(" in reader
    assert "F.theta_union(" in reader
    assert "F.theta_intersection(" in reader
    assert "F.theta_difference(" in reader
    assert "F.theta_union_agg(" in producer
    assert "F.theta_intersection_agg(" in producer
    assert "structure.logical_type" in schemas
    assert not any(token in producer for token in ("SparkContext", "sparkContext", "_jdf", "_jvm", ".rdd"))

    with generated_project(tmp_path, PACKAGE, files):
        source = spark.createDataFrame(
            [("a", 1.0, 1, 1.0, "x", "x"), ("a", 2.0, 2, 2.0, "y", "x"), ("a", 3.0, 3, 3.0, "x", "z"), ("a", None, 4, 4.0, None, None)],
            "group string, value double, long_value long, float_value float, member string, other_member string",
        )
        native = source.groupBy("group").agg(
            F.kll_sketch_agg_double("value", 64).alias("latency"),
            F.kll_sketch_agg_double("value", 32).alias("mixed_latency"),
            F.kll_sketch_agg_bigint("long_value", 64).alias("long_latency"),
            F.kll_sketch_agg_float("float_value", 64).alias("float_latency"),
            F.theta_sketch_agg("member", 12).alias("members"),
            F.theta_sketch_agg("other_member", 12).alias("other_members"),
        )
        online_state = SummarizeValues(rows=source).run(session(spark, execution_mode="online")).result
        generated_state = SummarizeValues(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        ).result
        online = ReadSketches(sketches=online_state).run(session(spark, execution_mode="online")).result
        generated = ReadSketches(sketches=generated_state).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        ).result
        online_rows = online.collect()
        generated_rows = generated.collect()
        combined_online_state = CombineThetaSketches(sketches=online_state).run(
            session(spark, execution_mode="online")
        ).result
        combined_generated_state = CombineThetaSketches(sketches=generated_state).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        ).result
        combined_online = ReadThetaAggregates(sketches=combined_online_state).run(
            session(spark, execution_mode="online")
        ).result
        combined_generated = ReadThetaAggregates(sketches=combined_generated_state).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        ).result
        native_row = native.first()
        native_values = (
            F.kll_sketch_get_n_double("latency").alias("count"),
            F.kll_sketch_get_quantile_double("latency", F.lit(0.5)).alias("median"),
            F.kll_sketch_get_rank_double("latency", F.lit(2.0)).alias("rank"),
            F.kll_sketch_get_quantile_double("latency", F.array(F.lit(0.25), F.lit(0.5), F.lit(0.75))).alias("quantiles"),
            F.kll_sketch_get_rank_double("latency", F.array(F.lit(1.0), F.lit(2.0), F.lit(3.0))).alias("ranks"),
            F.kll_sketch_to_string_double("latency").alias("rendering"),
            F.kll_sketch_get_quantile_bigint("long_latency", F.lit(0.5)).alias("long_median"),
            F.kll_sketch_get_quantile_float("float_latency", F.lit(0.5)).alias("float_median"),
            F.kll_sketch_get_n_double(F.kll_sketch_merge_double("latency", "mixed_latency")).alias("merged_count"),
            F.theta_sketch_estimate("members").alias("distinct_members"),
            F.theta_sketch_estimate(F.theta_union("members", "other_members", 12)).alias("union_members"),
            F.theta_sketch_estimate(F.theta_intersection("members", "other_members")).alias("intersection_members"),
            F.theta_sketch_estimate(F.theta_difference("members", "other_members")).alias("difference_members"),
        )
        native_metrics = native.select(*native_values).first()
        native_combined = native.groupBy("group").agg(
            F.theta_union_agg("members").alias("union_members"),
            F.theta_intersection_agg("members").alias("intersection_members"),
        ).select(
            F.theta_sketch_estimate("union_members").alias("union_members"),
            F.theta_sketch_estimate("intersection_members").alias("intersection_members"),
        ).first()
        assert online_state.schema["latency"].metadata == generated_state.schema["latency"].metadata
        parquet_path = Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"kll-theta-parquet-{uuid4().hex}"
        try:
            online_state.write.mode("overwrite").parquet(str(parquet_path))
            persisted_state = spark.read.parquet(str(parquet_path))
            assert persisted_state.schema["latency"].metadata == online_state.schema["latency"].metadata
            persisted_metrics = persisted_state.select(
                F.kll_sketch_get_n_double("latency").alias("count"),
                F.theta_sketch_estimate("members").alias("distinct_members"),
            ).first()
        finally:
            shutil.rmtree(parquet_path, ignore_errors=True)

    assert len(online_rows) == 1
    assert len(generated_rows) == 1
    for row in (online_rows[0], generated_rows[0]):
        assert (row["count"], row.median, row["merged_count"], row.distinct_members) == (3, 2.0, 6, 2)
        assert row.long_median == 2
        assert row.float_median == 2.0
        assert row.rank == native_metrics.rank
        assert list(row.quantiles) == list(native_metrics.quantiles)
        assert list(row.ranks) == list(native_metrics.ranks)
        assert row.rendering == native_metrics.rendering
        assert (row.union_members, row.intersection_members, row.difference_members) == (3, 1, 1)
        assert (row.filtered_count, row.filtered_members) == (2, 1)
    assert (combined_online.first().union_members, combined_online.first().intersection_members) == (2, 2)
    assert (combined_generated.first().union_members, combined_generated.first().intersection_members) == (2, 2)
    assert (native_combined.union_members, native_combined.intersection_members) == (2, 2)
    assert (persisted_metrics["count"], persisted_metrics.distinct_members) == (3, 2)
    assert native_row is not None
    assert (native_metrics["count"], native_metrics.median, native_metrics.distinct_members) == (3, 2.0, 2)


def test_empty_kll_count_and_query_error_match_native_online_and_generated(spark, tmp_path):
    from pyspark.sql import functions as F

    files = render_generated_projects(
        [
            (SummarizeEmptyKll, f"{SOURCE_MODULE}.SummarizeEmptyKll"),
            (ReadEmptyKll, f"{SOURCE_MODULE}.ReadEmptyKll"),
        ],
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [EmptyKllInput, EmptyKllState, EmptyKllMetrics]},
    )
    source = spark.createDataFrame([("empty", None)], "group string, value double")
    native = source.groupBy("group").agg(F.kll_sketch_agg_double("value").alias("sketch"))
    native_count = native.select(F.kll_sketch_get_n_double("sketch").alias("count")).first()["count"]

    with generated_project(tmp_path, PACKAGE, files):
        results = []
        for mode in ("online", "generated"):
            state = SummarizeEmptyKll(rows=source).run(
                session(spark, execution_mode=mode, generated_package=PACKAGE)
            ).result
            result = ReadEmptyKll(sketches=state).run(
                session(spark, execution_mode=mode, generated_package=PACKAGE)
            ).result
            results.append(result)
            assert result.select("count").first()["count"] == native_count == 0

    assert native_count == 0
    for result in results:
        with pytest.raises(Exception, match="KLL_SKETCH_INVALID_INPUT"):
            result.collect()
    with pytest.raises(Exception, match="KLL_SKETCH_INVALID_INPUT"):
        native.select(F.kll_sketch_get_quantile_double("sketch", F.lit(0.5))).first()


def test_kll_theta_streaming_aggregates_resume_with_the_same_configuration(spark, tmp_path):
    if backend_name().startswith("spark-connect"):
        pytest.skip("Structured Streaming is limited to ordinary PySpark")

    from pyspark.sql import functions as F

    files = render_generated_projects(
        [(SummarizeStreamingSketches, f"{SOURCE_MODULE}.SummarizeStreamingSketches")],
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [StreamingValues, StreamingSketchState]},
    )
    transform_source = files[f"{PACKAGE}/pyspark/transforms/integration/pyspark/v11/test_kll_theta_sketches.py"]
    assert "F.kll_sketch_agg_double(" in transform_source
    assert "F.theta_sketch_agg(" in transform_source
    assert not any(token in transform_source for token in ("SparkContext", "sparkContext", "_jdf", "_jvm", ".rdd"))

    root = Path(__file__).resolve().parents[4] / ".pytest-workspace-tmp" / "integration" / f"kll-theta-stream-{uuid4().hex}"
    expected_counts = ((2, 2), (4, 3))
    results = {}
    try:
        with generated_project(tmp_path, PACKAGE, files):
            from importlib import import_module

            schema_module = import_module(f"{PACKAGE}.pyspark.schemas.test_kll_theta_sketches")
            for mode in ("native", "online", "generated"):
                input_path = root / mode / "input"
                checkpoint = root / mode / "checkpoint"
                input_path.mkdir(parents=True)
                source = spark.readStream.schema(schema_module.STREAMING_VALUES_SCHEMA).json(str(input_path))
                if mode == "native":
                    state = source.withWatermark("event_time", "1 minute").groupBy(
                        F.window("event_time", "10 minutes").alias("bucket")
                    ).agg(
                        F.kll_sketch_agg_double("value", 64).alias("latency"),
                        F.theta_sketch_agg("member", 12).alias("members"),
                    )
                else:
                    state = SummarizeStreamingSketches(rows=source).run(
                        session(
                            spark,
                            execution_mode=mode,
                            generated_package=PACKAGE if mode == "generated" else None,
                        )
                    ).result

                latest_metrics: list[tuple[Any, int, builtins.float, int]] = []

                def collect_metrics(batch, _batch_id):
                    latest_metrics[:] = [
                        tuple(row)
                        for row in batch.select(
                            "bucket",
                            F.kll_sketch_get_n_double("latency").alias("count"),
                            F.kll_sketch_get_quantile_double("latency", F.lit(0.5)).alias("median"),
                            F.theta_sketch_estimate("members").alias("distinct_members"),
                        ).collect()
                    ]

                def run_available():
                    query = (
                        state.writeStream.foreachBatch(collect_metrics)
                        .outputMode("complete")
                        .option("checkpointLocation", str(checkpoint))
                        .trigger(availableNow=True)
                        .start()
                    )
                    try:
                        query.awaitTermination(120)
                    finally:
                        if query.isActive:
                            query.stop()

                _write_streaming_sketch_batch(
                    input_path / "batch-1.json",
                    [("a", "2026-01-01T10:01:00Z", 1.0, "x"), ("a", "2026-01-01T10:02:00Z", 2.0, "y")],
                )
                run_available()
                first_metrics = latest_metrics.copy()
                first_by_start = {row[0].start.isoformat(): row[1:] for row in first_metrics}
                first_window = first_by_start["2026-01-01T10:00:00"]
                assert (first_window[0], first_window[2]) == expected_counts[0]
                assert first_window[1] is not None

                _write_streaming_sketch_batch(
                    input_path / "batch-2.json",
                    [("a", "2026-01-01T10:03:00Z", 3.0, "y"), ("a", "2026-01-01T10:04:00Z", 4.0, "z")],
                )
                run_available()
                resumed_metrics = latest_metrics.copy()
                resumed_by_start = {row[0].start.isoformat(): row[1:] for row in resumed_metrics}
                first_window = resumed_by_start["2026-01-01T10:00:00"]
                assert (first_window[0], first_window[2]) == expected_counts[1]
                assert first_window[1] is not None
                results[mode] = resumed_metrics
    finally:
        shutil.rmtree(root, ignore_errors=True)

    assert results["online"] == results["native"]
    assert results["generated"] == results["native"]


def _write_streaming_sketch_batch(path: Path, rows: list[tuple[str, str, builtins.float, str]]) -> None:
    import json

    path.write_text(
        "\n".join(
            json.dumps({"group": group, "event_time": event_time, "value": value, "member": member})
            for group, event_time, value, member in rows
        ),
        encoding="utf-8",
    )
