from pyspark.sql import types as T

from testing.model.v2.structure_generated.orders.pyspark.schemas.common import TENANT_KEY_SCHEMA


CUSTOMER_DAILY_TOTAL_SCHEMA = T.StructType([
    T.StructField("tenant", TENANT_KEY_SCHEMA, False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("order_date", T.DateType(), True),
    T.StructField("order_count", T.LongType(), False),
    T.StructField("gross_total", T.DecimalType(12, 2), False),
    T.StructField("net_total", T.DecimalType(12, 2), False),
])

PRODUCT_DAILY_SUMMARY_SCHEMA = T.StructType([
    T.StructField("tenant", TENANT_KEY_SCHEMA, False),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("order_date", T.DateType(), True),
    T.StructField("order_count", T.LongType(), False),
    T.StructField("units", T.LongType(), False),
    T.StructField("gross_total", T.DecimalType(12, 2), False),
])
