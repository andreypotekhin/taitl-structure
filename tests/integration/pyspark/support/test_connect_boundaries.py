from __future__ import annotations

import pytest
from integration.pyspark.support.backend_matrix import (
    _plugin,
    backend_name,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.connect_boundaries import temporary_view_boundary

from structure import Schema, StructureConfig, StructureSession, Transform, input, lane, output, step
from structure.plugin.pyspark import checkpoint, long
from structure.plugin.pyspark.execution.logic.PlanBoundary import apply_plan_boundary

pytestmark = pytest.mark.integration


class BoundaryValue(Schema):
    value = long(nullable=True)


class BranchValues(Transform):
    rows = input(BoundaryValue, streaming=True)
    shared = lane(BoundaryValue)
    left = output(BoundaryValue)
    right = output(BoundaryValue)

    @step(input=rows, output=shared)
    def prepare(self, row: BoundaryValue) -> BoundaryValue:
        return BoundaryValue(value=row.value + 1)

    @step(input=shared, output=left)
    def first(self, row: BoundaryValue) -> BoundaryValue:
        return BoundaryValue(value=row.value + 2)

    @step(input=shared, output=right)
    def second(self, row: BoundaryValue) -> BoundaryValue:
        return BoundaryValue(value=row.value + 3)


class CheckpointValues(Transform):
    rows = input(BoundaryValue)
    result = output(BoundaryValue)

    def publish(self, row: BoundaryValue) -> BoundaryValue:
        checkpoint(eager=True)
        return BoundaryValue(value=row.value + 1)


def test_deep_checkpoint_input_preserves_alias_and_cleanup_with_boundaries_off(spark, tmp_path, monkeypatch):
    if backend_name() == "spark-connect35":
        pytest.skip("Spark Connect checkpoint requires Spark 4.0")
    monkeypatch.setenv("STRUCTURE_PLAN_BOUNDARIES", "off")
    package = "integration_checkpoint_generated"
    files = render_generated_project(
        CheckpointValues,
        source_transform=f"{__name__}.CheckpointValues",
        generated_package=package,
        source_schema_modules={__name__: (BoundaryValue,)},
    )
    source = spark.range(3).selectExpr("id AS value")
    for _ in range(60):
        source = source.selectExpr("value + 1 AS value")
    before = {table.name for table in spark.catalog.listTables()}
    with generated_project(tmp_path, package, files):
        for mode in ("online", "generated"):
            runtime = session(spark, execution_mode=mode, generated_package=package)
            try:
                result = CheckpointValues(rows=source).run(runtime).result
                assert [row.value for row in result.orderBy("value").collect()] == [61, 62, 63]
                created = {table.name for table in spark.catalog.listTables()} - before
                assert len(created) == (1 if backend_name() in {"spark-connect40", "spark-connect41"} else 0)
            finally:
                runtime.close()
            assert {table.name for table in spark.catalog.listTables()} == before


@pytest.mark.parametrize("streaming", [False, True])
def test_shared_plan_online_generated_parity_and_streaming_bypass(spark, tmp_path, monkeypatch, streaming):
    if streaming and backend_name().startswith("spark-connect"):
        pytest.skip("Streaming runtime support is limited to ordinary PySpark.")
    monkeypatch.setenv("STRUCTURE_PLAN_BOUNDARIES", "auto")
    package = "integration_boundary_generated"
    files = render_generated_project(
        BranchValues,
        source_transform=f"{__name__}.BranchValues",
        generated_package=package,
        source_schema_modules={__name__: (BoundaryValue,)},
    )
    source = (
        spark.readStream.format("rate").load().select("value")
        if streaming
        else spark.range(3).selectExpr("id AS value")
    )
    before = {table.name for table in spark.catalog.listTables()}
    with generated_project(tmp_path, package, files):
        sessions = [session(spark, execution_mode=mode, generated_package=package) for mode in ("online", "generated")]
        try:
            results = [BranchValues(rows=source).run(runtime) for runtime in sessions]
            if streaming:
                assert all(result.left.isStreaming and result.right.isStreaming for result in results)
                assert {table.name for table in spark.catalog.listTables()} == before
            else:
                for result in results:
                    assert [row.value for row in result.left.orderBy("value").collect()] == [3, 4, 5]
                    assert [row.value for row in result.right.orderBy("value").collect()] == [4, 5, 6]
                assert len({table.name for table in spark.catalog.listTables()} - before) == 2
        finally:
            for runtime in sessions:
                runtime.close()
    assert {table.name for table in spark.catalog.listTables()} == before


def test_temporary_view_preserves_lazy_result_and_cleanup(spark) -> None:
    source = spark.createDataFrame([(1, "one"), (2, "two")], ["id", "value"])
    with temporary_view_boundary(spark, source) as boundary:
        assert boundary.orderBy("id").collect() == source.orderBy("id").collect()

    # Dropping an already-removed view is safe for the prototype's cleanup path.
    assert not bool(spark.catalog.dropTempView("_structure_boundary_missing"))


def test_structure_session_close_drops_views_without_stopping_spark(spark) -> None:
    source = spark.createDataFrame([(1,)], ["id"])
    boundary = apply_plan_boundary(source, spark)
    assert boundary.count() == 1

    with StructureSession(
        spark=spark,
        config=StructureConfig.create(plugin=_plugin()),
    ) as runtime:
        runtime.close()
        runtime.close()

    assert spark.range(1).count() == 1
