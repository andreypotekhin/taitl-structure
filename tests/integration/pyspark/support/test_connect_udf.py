from __future__ import annotations

import pytest
from integration.pyspark.support.backend_matrix import backend_name


def test_spark_connect_executes_scalar_python_udf(spark) -> None:
    if not backend_name().startswith("spark-connect"):
        pytest.skip("Spark Connect capability probe")

    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    uppercase = F.udf(lambda value: value.upper(), T.StringType())
    rows = spark.createDataFrame([("connect",)], ["value"]).select(uppercase("value").alias("value")).collect()

    assert rows[0]["value"] == "CONNECT"


def test_spark_connect_executor_can_import_project_modules(spark) -> None:
    if not backend_name().startswith("spark-connect"):
        pytest.skip("Spark Connect executor import-path capability probe")

    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    module_name = F.udf(lambda _: __import__("examples").__name__, T.StringType())
    rows = spark.createDataFrame([(1,)], ["value"]).select(module_name("value").alias("module")).collect()

    assert rows[0]["module"] == "examples"
