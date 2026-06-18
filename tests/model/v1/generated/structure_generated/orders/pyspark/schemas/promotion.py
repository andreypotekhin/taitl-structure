from pyspark.sql import types as T


PROMOTION_SCHEMA = T.StructType([
    T.StructField("tenant_id", T.StringType(), False),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("ingested_at", T.TimestampType(), False),
    T.StructField("code", T.StringType(), False),
    T.StructField("name", T.StringType(), True),
    T.StructField("discount", T.DecimalType(12, 2), True),
])
