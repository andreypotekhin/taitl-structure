from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from testing.model.v0.structure_generated.orders.pyspark.schemas.order import ORDER_NORMALIZED_SCHEMA, ORDER_RAW_SCHEMA
from testing.model.v0.structure_generated.runtime.schema_assert import assert_schema


class NormalizeOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx

    def run(self, *, orders: DataFrame) -> DataFrame:
        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")

        total = F.coalesce(F.col("total").cast("decimal(12,2)"), F.lit(0).cast("decimal(12,2)"))
        df = orders.where(
            F.col("id").isNotNull()
            & F.col("customer_id").isNotNull()
        ).select(
            F.lower(F.trim(F.col("id"))).alias("id"),
            F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
            total.alias("total"),
        )

        assert_schema(df, ORDER_NORMALIZED_SCHEMA, name="OrderNormalized", mode="strict")
        return df


def normalize_orders(*, orders: DataFrame, spark: SparkSession, ctx=None) -> DataFrame:
    return NormalizeOrdersGenerated(spark=spark, ctx=ctx).run(orders=orders)
