from pyspark.sql import types as T

from structure_generated.orders.pyspark.schemas.common import AUDIT_STAMP_SCHEMA, TENANT_KEY_SCHEMA


CUSTOMER_SCHEMA = T.StructType([
    T.StructField("tenant", TENANT_KEY_SCHEMA, False),
    T.StructField("audit", AUDIT_STAMP_SCHEMA, False),
    T.StructField("id", T.StringType(), False),
    T.StructField("name", T.StringType(), True),
    T.StructField("tier", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
])
