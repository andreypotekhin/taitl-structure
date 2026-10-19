from __future__ import annotations

import importlib
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_projects,
    session,
)

from structure import Schema, Transform, input, output, special, transform
from structure.plugin.pyspark import current_row, double, group_by, long, preceding, rows_between, string, window
from structure.plugin.pyspark.dsl import types

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        backend_name() not in {"pyspark41", "spark-connect41"},
        reason="Arrow UDF parity requires exact PySpark 4.1 profiles",
    ),
]

SOURCE_MODULE = "integration.pyspark.v11.test_arrow_udfs"


class Values(Schema):
    key = long(nullable=False)
    value = long(nullable=True)


class ScalarResult(Schema):
    key = long(nullable=False)
    result = long(nullable=True)
    incremented = long(nullable=True)


class GroupResult(Schema):
    key = long(nullable=False)
    mean = double(nullable=True)


class WindowResult(Schema):
    key = long(nullable=False)
    value = long(nullable=True)
    mean = double(nullable=True)


class TextResult(Schema):
    key = long(nullable=False)
    value = string(nullable=True)


class StreamResult(Schema):
    key = long(nullable=False)
    scalar_value = long(nullable=True)
    iterator_value = long(nullable=True)


@transform
class ArrowScalars(Transform):
    rows = input(Values)
    output_rows = output(ScalarResult)

    @special(type="arrow_udf", return_type=types.long())
    def add(left, right):
        import pyarrow.compute as pc  # type: ignore[import-not-found]

        return pc.add(left, right)

    @special(type="arrow_udf", return_type=types.long(), function_type="scalar_iter", deterministic=False)
    def increment(batches: Any):
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.compute as pc  # type: ignore[import-not-found]

        return (pc.add(batch, pa.scalar(1, type=pa.int64())) for batch in batches)

    def apply(self, row: Values) -> ScalarResult:
        return ScalarResult(
            key=row.key,
            result=self.add(left=row.value, right=row.value),
            incremented=self.increment(row.value),
        )


@transform
class ArrowGrouped(Transform):
    rows = input(Values)
    output_rows = output(GroupResult)

    @special(type="arrow_udf", return_type=types.double(), function_type="grouped_agg")
    def mean(values):
        import pyarrow.compute as pc  # type: ignore[import-not-found]

        return pc.mean(values).as_py()

    def apply(self, row: Values) -> GroupResult:
        group_by(row.key)
        return GroupResult(key=row.key, mean=self.mean(row.value))


@transform
class ArrowWindow(Transform):
    rows = input(Values)
    output_rows = output(WindowResult)

    @special(type="arrow_udf", return_type=types.double(), function_type="grouped_agg")
    def mean(values):
        import pyarrow.compute as pc  # type: ignore[import-not-found]

        return pc.mean(values).as_py()

    def apply(self, row: Values) -> WindowResult:
        rolling = window(
            partition_by=row.key,
            order_by=row.value,
            frame=rows_between(preceding(1), current_row()),
        )
        return WindowResult(key=row.key, value=row.value, mean=self.mean(row.value, over=rolling))


@transform
class ArrowTypeChange(Transform):
    rows = input(Values)
    output_rows = output(TextResult)

    @special(type="arrow_udf", return_type=types.string())
    def as_text(values):
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.compute as pc  # type: ignore[import-not-found]

        return pc.cast(values, pa.string())

    def apply(self, row: Values) -> TextResult:
        return TextResult(key=row.key, value=self.as_text(row.value))


@transform(streaming=True)
class ArrowStreaming(Transform):
    rows = input(Values, streaming=True)
    output_rows = output(StreamResult)

    @special(type="arrow_udf", return_type=types.long())
    def increment(values):
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.compute as pc  # type: ignore[import-not-found]

        return pc.add(values, pa.scalar(1, type=pa.int64()))

    @special(type="arrow_udf", return_type=types.long(), function_type="scalar_iter")
    def increment_batches(batches: Any):
        import pyarrow as pa
        import pyarrow.compute as pc

        return (pc.add(batch, pa.scalar(1, type=pa.int64())) for batch in batches)

    def apply(self, row: Values) -> StreamResult:
        return StreamResult(
            key=row.key,
            scalar_value=self.increment(row.value),
            iterator_value=self.increment_batches(row.value),
        )


def _generated(package: str, transforms):
    return render_generated_projects(
        [(transform_type, f"{SOURCE_MODULE}.{transform_type.__name__}") for transform_type in transforms],
        generated_package=package,
        source_schema_modules={SOURCE_MODULE: [Values, ScalarResult, GroupResult, WindowResult, TextResult, StreamResult]},
    )


def test_scalar_and_iterator_arrow_udfs_match_native_online_and_generated(spark, tmp_path) -> None:
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.compute as pc  # type: ignore[import-not-found]
    from pyspark.sql import functions as F
    from pyspark.sql.functions import ArrowUDFType

    source = spark.createDataFrame([(1, 2), (1, None), (2, 5)], "key long not null, value long")
    native_add = F.arrow_udf(lambda left, right: pc.add(left, right), returnType="long", functionType=ArrowUDFType.SCALAR)
    native_increment = F.arrow_udf(
        lambda batches: (pc.add(batch, pa.scalar(1, type=pa.int64())) for batch in batches),
        returnType="long",
        functionType=ArrowUDFType.SCALAR_ITER,
    ).asNondeterministic()
    native = source.select(
        "key",
        native_add(right=F.col("value"), left=F.col("value")).alias("result"),
        native_increment(F.col("value")).alias("incremented"),
    )
    package = "integration_v11_arrow_scalar_generated"
    files = _generated(package, (ArrowScalars,))
    generated_source = "\n".join(files.values())
    assert ".arrow_udf(" in generated_source
    assert "F.ArrowUDFType.SCALAR" in generated_source
    assert "F.ArrowUDFType.SCALAR_ITER" in generated_source
    assert ".asNondeterministic" in generated_source
    assert not any(token in generated_source for token in ("_jdf", "_jvm", "SparkContext", "sparkContext", ".rdd"))

    with generated_project(tmp_path, package, files):
        importlib.import_module(f"{package}.pyspark.schemas.test_arrow_udfs")
        importlib.import_module(f"{package}.pyspark.transforms.integration.pyspark.v11.test_arrow_udfs")
        online = ArrowScalars(rows=source).run(session(spark, execution_mode="online"))
        generated = ArrowScalars(rows=source).run(
            session(spark, execution_mode="generated", generated_package=package)
        )
        expected = native.collect()
        online_rows = online.output_rows.collect()
        generated_rows = generated.output_rows.collect()

    def values(rows):
        return sorted(
            ((row.key, row.result, row.incremented) for row in rows),
            key=lambda row: row[0],
        )

    assert values(online_rows) == values(expected) == values(generated_rows)
    assert online.output_rows.schema == generated.output_rows.schema


def test_grouped_and_window_arrow_udfs_match_native_online_and_generated(spark, tmp_path) -> None:
    import pyarrow.compute as pc  # type: ignore[import-not-found]
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.functions import ArrowUDFType

    source = spark.createDataFrame([(1, 1), (1, 3), (2, 5)], "key long not null, value long")
    native_mean = F.arrow_udf(lambda values: pc.mean(values).as_py(), returnType="double", functionType=ArrowUDFType.GROUPED_AGG)
    native_grouped = source.groupBy("key").agg(native_mean(F.col("value")).alias("mean"))
    native_window = source.select(
        "key",
        "value",
        native_mean(F.col("value")).over(
            Window.partitionBy("key").orderBy("value").rowsBetween(-1, 0)
        ).alias("mean"),
    )
    package = "integration_v11_arrow_aggregate_generated"
    files = _generated(package, (ArrowGrouped, ArrowWindow))
    assert "F.ArrowUDFType.GROUPED_AGG" in "\n".join(files.values())

    with generated_project(tmp_path, package, files):
        importlib.import_module(f"{package}.pyspark.schemas.test_arrow_udfs")
        importlib.import_module(f"{package}.pyspark.transforms.integration.pyspark.v11.test_arrow_udfs")
        grouped_online = ArrowGrouped(rows=source).run(session(spark, execution_mode="online"))
        grouped_generated = ArrowGrouped(rows=source).run(
            session(spark, execution_mode="generated", generated_package=package)
        )
        window_online = ArrowWindow(rows=source).run(session(spark, execution_mode="online"))
        window_generated = ArrowWindow(rows=source).run(
            session(spark, execution_mode="generated", generated_package=package)
        )
        expected_groups = sorted((row.key, row.mean) for row in native_grouped.collect())
        expected_window = sorted((row.key, row.value, row.mean) for row in native_window.collect())
        actual_groups = [
            sorted((row.key, row.mean) for row in result.output_rows.collect())
            for result in (grouped_online, grouped_generated)
        ]
        actual_window = [
            sorted((row.key, row.value, row.mean) for row in result.output_rows.collect())
            for result in (window_online, window_generated)
        ]

    assert actual_groups == [expected_groups, expected_groups] == [[(1, 2.0), (2, 5.0)]] * 2
    assert actual_window == [expected_window, expected_window] == [[(1, 1, 1.0), (1, 3, 2.0), (2, 5, 5.0)]] * 2
    assert grouped_online.output_rows.schema == grouped_generated.output_rows.schema
    assert window_online.output_rows.schema == window_generated.output_rows.schema


def test_arrow_udf_can_change_the_declared_result_type(spark, tmp_path) -> None:
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.compute as pc  # type: ignore[import-not-found]
    from pyspark.sql import functions as F
    from pyspark.sql.functions import ArrowUDFType

    source = spark.createDataFrame([(1, 22), (2, None)], "key long not null, value long")
    native_cast = F.arrow_udf(
        lambda values: pc.cast(values, pa.string()),
        returnType="string",
        functionType=ArrowUDFType.SCALAR,
    )
    native = source.select("key", native_cast(F.col("value")).alias("value"))
    package = "integration_v11_arrow_type_change_generated"
    files = _generated(package, (ArrowTypeChange,))

    with generated_project(tmp_path, package, files):
        importlib.import_module(f"{package}.pyspark.schemas.test_arrow_udfs")
        importlib.import_module(f"{package}.pyspark.transforms.integration.pyspark.v11.test_arrow_udfs")
        online = ArrowTypeChange(rows=source).run(session(spark, execution_mode="online"))
        generated = ArrowTypeChange(rows=source).run(
            session(spark, execution_mode="generated", generated_package=package)
        )
        expected_rows = sorted(native.collect(), key=lambda row: row.key)
        online_rows = sorted(online.output_rows.collect(), key=lambda row: row.key)
        generated_rows = sorted(generated.output_rows.collect(), key=lambda row: row.key)

    assert [(row.key, row.value) for row in online_rows] == [(row.key, row.value) for row in expected_rows]
    assert [(row.key, row.value) for row in generated_rows] == [(row.key, row.value) for row in expected_rows]
    assert online.output_rows.schema == generated.output_rows.schema


@pytest.mark.skipif(backend_name() != "pyspark41", reason="Spark Connect streaming is outside the Arrow UDF contract")
def test_scalar_and_iterator_arrow_streams_resume_from_the_same_durable_checkpoint(
    spark, tmp_path, integration_shared_dir
) -> None:
    import pyarrow as pa
    import pyarrow.compute as pc
    from pyspark.sql import functions as F
    from pyspark.sql.functions import ArrowUDFType

    package = "integration_v11_arrow_stream_generated"
    files = _generated(package, (ArrowStreaming,))
    native_scalar = F.arrow_udf(
        lambda values: pc.add(values, pa.scalar(1, type=pa.int64())),
        returnType="long",
        functionType=ArrowUDFType.SCALAR,
    )
    native_iterator = F.arrow_udf(
        lambda batches: (pc.add(batch, pa.scalar(1, type=pa.int64())) for batch in batches),
        returnType="long",
        functionType=ArrowUDFType.SCALAR_ITER,
    )

    def source_at(path):
        return spark.readStream.schema("key long not null, value long").option("maxFilesPerTrigger", 1).json(str(path))

    def add_batch(path, name, rows):
        path.mkdir(parents=True, exist_ok=True)
        text = "\n".join(f'{{"key": {key}, "value": {value}}}' for key, value in rows)
        (path / name).write_text(text + "\n", encoding="utf-8")

    def run_restart(mode, root, *, generated_package=None):
        root = root / mode
        inputs = root / "input"
        output_path = root / "output"
        checkpoint = root / "checkpoint"
        add_batch(inputs, "batch-1.json", [(1, 1), (1, 3)])

        def transformed():
            current = source_at(inputs)
            if mode == "native":
                return current.select(
                    "key",
                    native_scalar(F.col("value")).alias("scalar_value"),
                    native_iterator(F.col("value")).alias("iterator_value"),
                )
            result = ArrowStreaming(rows=current).run(
                session(spark, execution_mode=mode, generated_package=generated_package)
            )
            return result.output_rows

        def start():
            return (
                transformed()
                .writeStream.format("parquet")
                .option("path", str(output_path))
                .option("checkpointLocation", str(checkpoint))
                .start()
            )

        first = start()
        try:
            first.processAllAvailable()
        finally:
            first.stop()
        add_batch(inputs, "batch-2.json", [(2, 5), (2, 9)])
        resumed = start()
        try:
            resumed.processAllAvailable()
        finally:
            resumed.stop()
        return sorted(
            (row.key, row.scalar_value, row.iterator_value)
            for row in spark.read.parquet(str(output_path)).collect()
        )

    with generated_project(tmp_path, package, files):
        importlib.import_module(f"{package}.pyspark.schemas.test_arrow_udfs")
        importlib.import_module(f"{package}.pyspark.transforms.integration.pyspark.v11.test_arrow_udfs")
        with TemporaryDirectory(prefix=f"arrow-stream-{uuid4().hex}-", dir=integration_shared_dir) as shared_root:
            results = {
                mode: run_restart(
                    mode,
                    Path(shared_root),
                    generated_package=package if mode == "generated" else None,
                )
                for mode in ("native", "online", "generated")
            }

    expected = [(1, 2, 2), (1, 4, 4), (2, 6, 6), (2, 10, 10)]
    assert results == {"native": expected, "online": expected, "generated": expected}
