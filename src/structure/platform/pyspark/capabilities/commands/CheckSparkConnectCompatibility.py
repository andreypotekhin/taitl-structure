from structure.platform.pyspark.capabilities.logic.SparkConnectCompatibility import (
    is_classic_only_spark_error,
    is_spark_connect_session,
)


class CheckSparkConnectCompatibility:

    def classic_only_error(self, error: Exception) -> bool:
        return is_classic_only_spark_error(error)

    def session(self, *, session=None, spark=None) -> bool:
        return is_spark_connect_session(session=session, spark=spark)
