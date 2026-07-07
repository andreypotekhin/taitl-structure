from __future__ import annotations

import os

import pytest
from integration.pyspark.support.backend_matrix import BACKENDS

pytestmark = pytest.mark.integration


def test_runtime_versions(spark) -> None:
    pyspark = pytest.importorskip("pyspark")
    backend = os.environ.get("STRUCTURE_INTEGRATION_BACKEND")
    expected_pyspark = os.environ.get("STRUCTURE_EXPECTED_PYSPARK")
    expected_spark = os.environ.get("STRUCTURE_EXPECTED_SPARK")

    assert backend in BACKENDS
    assert expected_pyspark is not None
    assert expected_spark is not None
    assert pyspark.__version__.startswith(expected_pyspark)
    assert spark.version.startswith(expected_spark)
