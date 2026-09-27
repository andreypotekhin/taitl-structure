import pytest

from structure.plugin.pyspark.capabilities.logic.SparkConnectCompatibility import is_classic_only_spark_error


@pytest.mark.parametrize("message", [
    "[USER_RAISED_EXCEPTION] REL-E0702: require_unique(...) found duplicate keys",
    "[DIVIDE_BY_ZERO] Division by zero",
])
def test_server_stack_frames_do_not_mask_data_errors(message):
    error = RuntimeError(
        f"{message}\n\nJVM stacktrace:\norg.apache.spark.SparkRuntimeException\n"
        "\tat org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:374)\n"
        "\tat org.apache.spark.SparkContext.runJob(SparkContext.scala:10)"
    )
    assert not is_classic_only_spark_error(error)


@pytest.mark.parametrize("message", [
    "[JVM_ATTRIBUTE_NOT_SUPPORTED] Attribute `_jvm` is not supported in Spark Connect",
    "Generated hook touched _jvm through Py4J",
    "RDD is not supported in Spark Connect",
])
def test_actual_classic_only_api_errors_remain_classified(message):
    assert is_classic_only_spark_error(RuntimeError(message))
