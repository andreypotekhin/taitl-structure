from pyspark.sql.types import DecimalType, StringType, StructField, StructType


ORDER_RAW_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("total", StringType(), True),
    ]
)

ORDER_NORMALIZED_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("total", DecimalType(12, 2), False),
    ]
)
