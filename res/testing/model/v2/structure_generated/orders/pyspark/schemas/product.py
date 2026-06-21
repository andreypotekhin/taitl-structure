from pyspark.sql import types as T

from testing.model.v2.structure_generated.orders.pyspark.schemas.common import AUDIT_STAMP_SCHEMA, TENANT_KEY_SCHEMA


PRODUCT_SCHEMA = T.StructType([
    T.StructField("tenant", TENANT_KEY_SCHEMA, False),
    T.StructField("audit", AUDIT_STAMP_SCHEMA, False),
    T.StructField("id", T.StringType(), False),
    T.StructField("name", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("active", T.BooleanType(), False),
    T.StructField("list_price", T.DecimalType(12, 2), True),
    T.StructField("weight", T.FloatType(), True),
    T.StructField("rating", T.DoubleType(), True),
])
