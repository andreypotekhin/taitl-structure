from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from integration.pyspark.support.backend_matrix import BACKENDS

from structure.core.runtime.session.model.StructureRuntimeError import StructureRuntimeError
from structure.plugin.pyspark.execution.logic.ValidatePySparkRuntime import validate_pyspark_runtime

pytestmark = pytest.mark.integration


def test_runtime_versions(spark, capsys) -> None:
    pyspark = pytest.importorskip("pyspark")
    backend = os.environ.get("STRUCTURE_INTEGRATION_BACKEND")
    expected_pyspark = os.environ.get("STRUCTURE_EXPECTED_PYSPARK")
    expected_spark = os.environ.get("STRUCTURE_EXPECTED_SPARK")

    with capsys.disabled():
        print(f"Runtime: backend={backend}; PySpark={pyspark.__version__}; Spark={spark.version}")

    assert backend in BACKENDS
    assert expected_pyspark is not None
    assert expected_spark is not None
    assert pyspark.__version__.startswith(expected_pyspark)
    assert spark.version.startswith(expected_spark)

    profile_by_runtime = {
        "3.5": ">=3.5,<4.0",
        "4.0": ">=4.0,<4.1",
        "4.1": ">=4.1,<4.2",
    }
    runtime_line = next((line for line in profile_by_runtime if expected_spark.startswith(line)), None)
    assert runtime_line is not None
    profile = profile_by_runtime[runtime_line]
    validate_pyspark_runtime(
        SimpleNamespace(
            runtime=spark,
            plugin_options={"profile": profile, "variant": "ordinary", "runtime_version_check": "raise"},
        ),
        transform="integration.runtime_versions",
    )

    mismatched = ">=4.1,<4.2" if expected_spark.startswith("3.5") else ">=3.5,<4.0"
    with pytest.raises(StructureRuntimeError) as raised:
        validate_pyspark_runtime(
            SimpleNamespace(
                runtime=spark,
                plugin_options={"profile": mismatched, "variant": "ordinary", "runtime_version_check": "raise"},
            ),
            transform="integration.runtime_versions",
        )
    assert raised.value.diagnostic.code == "PYSPARK-E2710"
