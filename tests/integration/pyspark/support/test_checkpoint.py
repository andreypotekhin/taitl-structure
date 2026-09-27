import pytest
from integration.pyspark.support.backend_matrix import backend_name


def test_reliable_checkpoint_preserves_rows_and_severs_parent(spark, capsys) -> None:
    if backend_name() == "spark-connect35":
        pytest.skip("Spark Connect checkpoint requires Spark 4.0")

    original = spark.range(3).selectExpr("id", "id + 1 AS next_id")
    checkpointed = original.checkpoint(eager=True)
    checkpointed.explain(extended=True)
    explanation = capsys.readouterr().out
    assert "LogicalRDD" in explanation
    assert "Range (0, 3" not in explanation
    assert checkpointed.schema == original.schema
    assert checkpointed.orderBy("id").collect() == original.orderBy("id").collect()
    # The checkpoint must remain usable after its input's session view is removed.
    original.createOrReplaceTempView("checkpoint_source")
    from_view = spark.table("checkpoint_source").checkpoint(eager=True)
    spark.catalog.dropTempView("checkpoint_source")
    assert from_view.orderBy("id").collect() == checkpointed.orderBy("id").collect()
