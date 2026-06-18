from pyspark.sql import types as T


PRODUCT_SCHEMA = T.StructType([
    T.StructField("tenant_id", T.StringType(), False),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("ingested_at", T.TimestampType(), False),
    T.StructField("id", T.StringType(), False),
    T.StructField("name", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("active", T.BooleanType(), False),
    T.StructField("list_price", T.DecimalType(12, 2), True),
    T.StructField("weight", T.FloatType(), True),
    T.StructField("rating", T.DoubleType(), True),
])
