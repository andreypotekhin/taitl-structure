from pyspark.sql import types as T


SHIPMENT_SCHEMA = T.StructType([
    T.StructField("tenant_id", T.StringType(), False),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("ingested_at", T.TimestampType(), False),
    T.StructField("order_id", T.StringType(), False),
    T.StructField("line_number", T.IntegerType(), False),
    T.StructField("carrier", T.StringType(), True),
    T.StructField("tracking_number", T.StringType(), True),
    T.StructField("shipped_at", T.TimestampType(), True),
])
