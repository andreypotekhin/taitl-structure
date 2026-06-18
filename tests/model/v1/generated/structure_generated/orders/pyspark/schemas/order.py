from pyspark.sql import types as T


SHIPPING_SCHEMA = T.StructType([
    T.StructField("line1", T.StringType(), False),
    T.StructField("line2", T.StringType(), True),
    T.StructField("city", T.StringType(), False),
    T.StructField("state", T.StringType(), True),
    T.StructField("postal_code", T.StringType(), False),
    T.StructField("country", T.StringType(), False),
])

ORDER_RAW_SCHEMA = T.StructType([
    T.StructField("tenant_id", T.StringType(), False),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("ingested_at", T.TimestampType(), False),
    T.StructField("order_date", T.DateType(), True),
    T.StructField("id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("promotion_code", T.StringType(), True),
    T.StructField("total", T.StringType(), True),
    T.StructField("discount", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("tags", T.ArrayType(T.StringType(), containsNull=False), True),
    T.StructField("attributes", T.MapType(T.StringType(), T.StringType(), valueContainsNull=True), True),
    T.StructField("shipping", SHIPPING_SCHEMA, True),
])

ORDER_NORMALIZED_SCHEMA = T.StructType([
    T.StructField("tenant_id", T.StringType(), False),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("ingested_at", T.TimestampType(), False),
    T.StructField("order_date", T.DateType(), True),
    T.StructField("id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("promotion_code", T.StringType(), True),
    T.StructField("total", T.DecimalType(12, 2), False),
    T.StructField("discount", T.DecimalType(12, 2), False),
    T.StructField("net_total", T.DecimalType(12, 2), False),
    T.StructField("quantity", T.LongType(), False),
    T.StructField("tags", T.ArrayType(T.StringType(), containsNull=False), True),
    T.StructField("attributes", T.MapType(T.StringType(), T.StringType(), valueContainsNull=True), True),
    T.StructField("shipping", SHIPPING_SCHEMA, True),
    T.StructField("is_large", T.BooleanType(), False),
])

ORDER_WITH_CUSTOMER_SCHEMA = T.StructType(ORDER_NORMALIZED_SCHEMA.fields + [
    T.StructField("customer_name", T.StringType(), True),
    T.StructField("customer_tier", T.StringType(), True),
    T.StructField("customer_region", T.StringType(), True),
])

ORDER_WITH_PRODUCT_SCHEMA = T.StructType(ORDER_WITH_CUSTOMER_SCHEMA.fields + [
    T.StructField("product_name", T.StringType(), True),
    T.StructField("product_category", T.StringType(), True),
    T.StructField("product_active", T.BooleanType(), True),
    T.StructField("product_list_price", T.DecimalType(12, 2), True),
])

ORDER_WITH_PROMOTION_SCHEMA = T.StructType(ORDER_WITH_PRODUCT_SCHEMA.fields + [
    T.StructField("promotion_name", T.StringType(), True),
    T.StructField("promotion_discount", T.DecimalType(12, 2), True),
])

ORDER_FULFILLMENT_SCHEMA = T.StructType(ORDER_WITH_PROMOTION_SCHEMA.fields + [
    T.StructField("shipment_line", T.IntegerType(), False),
    T.StructField("carrier", T.StringType(), True),
    T.StructField("tracking_number", T.StringType(), True),
    T.StructField("shipped_at", T.TimestampType(), True),
])

ORDER_PUBLISHED_SCHEMA = T.StructType([
    T.StructField("tenant_id", T.StringType(), False),
    T.StructField("id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("customer_name", T.StringType(), True),
    T.StructField("customer_tier", T.StringType(), True),
    T.StructField("product_name", T.StringType(), True),
    T.StructField("product_category", T.StringType(), True),
    T.StructField("promotion_name", T.StringType(), True),
    T.StructField("total", T.DecimalType(12, 2), False),
    T.StructField("discount", T.DecimalType(12, 2), False),
    T.StructField("net_total", T.DecimalType(12, 2), False),
    T.StructField("quantity", T.LongType(), False),
    T.StructField("order_date", T.DateType(), True),
    T.StructField("carrier", T.StringType(), True),
    T.StructField("tracking_number", T.StringType(), True),
    T.StructField("shipped_at", T.TimestampType(), True),
    T.StructField("is_large", T.BooleanType(), False),
    T.StructField("has_promotion", T.BooleanType(), False),
])
