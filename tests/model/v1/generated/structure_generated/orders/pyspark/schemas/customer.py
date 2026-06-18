from pyspark.sql import types as T


CUSTOMER_SCHEMA = T.StructType([
    T.StructField("tenant_id", T.StringType(), False),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("ingested_at", T.TimestampType(), False),
    T.StructField("id", T.StringType(), False),
    T.StructField("name", T.StringType(), True),
    T.StructField("tier", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
])
