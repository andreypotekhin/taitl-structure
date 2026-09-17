import pytest
from helpers.string_addition import StringAddition, StringResult, StringSource
from integration.pyspark.support.backend_matrix import (
    assert_generated_connect_safe,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.backend_matrix import spark as _spark

from structure.lib.testing import assert_online_generated_parity

spark_fixture = pytest.fixture(name="spark")(getattr(_spark, "__wrapped__"))
pytestmark = pytest.mark.integration


def test_string_addition_parity(spark, tmp_path) -> None:
    """String + matches native concat through direct and generated execution, including SQL nulls."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    package = "concept_string_addition_generated"
    files = render_generated_project(
        StringAddition,
        source_transform="helpers.string_addition.StringAddition",
        generated_package=package,
        source_schema_modules={"helpers.string_addition": [StringSource, StringResult]},
    )
    schema = T.StructType([
        T.StructField("id", T.IntegerType(), False),
        T.StructField("first", T.StringType(), True),
        T.StructField("last", T.StringType(), True),
    ])
    frame = spark.createDataFrame([
        (1, "Ada", "Lovelace"),
        (2, "12", "3"),
        (3, "", ""),
        (4, "café", "東京"),
        (5, None, "last"),
        (6, "first", None),
        (7, None, None),
    ], schema)
    reference = frame.select(
        F.col("id"),
        F.concat(F.col("first"), F.col("last")).alias("joined"),
        F.concat(F.lit("order:"), F.col("first")).alias("prefixed"),
        F.concat(F.col("first"), F.lit("!")).alias("suffixed"),
        F.concat(F.col("first"), F.lit(" / "), F.col("last")).alias("chained"),
        F.concat(F.coalesce(F.col("first"), F.lit("")), F.lit("!")).alias("fallback"),
        F.concat(F.lit("n="), F.col("id").cast("string")).alias("converted"),
        F.concat(F.col("first"), F.lit(None).cast("string")).alias("null_string"),
        (F.col("id") + F.lit(1)).alias("incremented"),
    )
    with generated_project(tmp_path, package, files):
        def online():
            return StringAddition(rows=frame).run(session(spark, execution_mode="online"))

        def generated():
            return StringAddition(rows=frame).run(
                session(spark, execution_mode="generated", generated_package=package)
            )

        assert_online_generated_parity(online, generated, outputs=("result",))
        result = generated().result
        assert result.schema == reference.schema
        actual = result.orderBy("id").collect()
        assert actual == reference.orderBy("id").collect()
        assert actual[1].joined == "123"
        assert actual[3].joined == "café東京"
        assert actual[4].joined is None
        assert actual[4].fallback == "!"
    assert_generated_connect_safe(files)
