from __future__ import annotations

import pytest
from integration.pyspark.support.backend_matrix import backend_name
from integration.pyspark.support.connect_boundaries import temporary_view_boundary

from structure import StructureConfig, StructureSession
from structure.plugin.pyspark.execution.logic.PlanBoundary import apply_plan_boundary

pytestmark = pytest.mark.integration


def test_connect_temporary_view_preserves_lazy_result_and_cleanup(spark) -> None:
    if not backend_name().startswith("spark-connect"):
        pytest.skip("Temporary-view boundary prototype targets Spark Connect.")

    source = spark.createDataFrame([(1, "one"), (2, "two")], ["id", "value"])
    with temporary_view_boundary(spark, source) as boundary:
        assert boundary.orderBy("id").collect() == source.orderBy("id").collect()

    # Dropping an already-removed view is safe for the prototype's cleanup path.
    assert not bool(spark.catalog.dropTempView("_structure_boundary_missing"))


def test_structure_session_close_drops_views_without_stopping_connect(spark) -> None:
    if not backend_name().startswith("spark-connect"):
        pytest.skip("Temporary-view boundary prototype targets Spark Connect.")

    source = spark.createDataFrame([(1,)], ["id"])
    boundary = apply_plan_boundary(source, spark)
    assert boundary.count() == 1

    StructureSession(
        spark=spark,
        config=StructureConfig.create(plugin={"pyspark": {"variant": "spark-connect"}}),
    ).close()
    assert spark.range(1).count() == 1
