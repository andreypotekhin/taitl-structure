from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orders.transforms.analytics import OrderAnalytics
from structure_generated.orders.pyspark.schemas.analytics import (
    CUSTOMER_DAILY_TOTAL_SCHEMA,
    PRODUCT_DAILY_SUMMARY_SCHEMA,
)
from structure_generated.orders.pyspark.schemas.order import ORDER_FULFILLMENT_SCHEMA
from structure_generated.runtime.schema_assert import assert_schema


class OrderAnalyticsGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = OrderAnalytics()

    def customer_daily_totals(self, *, fulfilled: DataFrame) -> DataFrame:
        assert_schema(fulfilled, ORDER_FULFILLMENT_SCHEMA, name="OrderFulfillment", mode="strict")

        df = fulfilled.groupBy(
            F.col("tenant.tenant_id").alias("tenant_id"),
            F.col("customer_id").alias("customer_id"),
            F.col("business.order_date").alias("order_date"),
        ).agg(
            F.first(F.col("tenant"), ignorenulls=False).alias("tenant"),
            F.count(F.lit(1)).cast("long").alias("order_count"),
            F.sum(F.col("total")).cast("decimal(12,2)").alias("gross_total"),
            F.sum(F.col("net_total")).cast("decimal(12,2)").alias("net_total"),
        ).select(
            F.col("tenant").alias("tenant"),
            F.col("customer_id").alias("customer_id"),
            F.col("order_date").alias("order_date"),
            F.col("order_count").alias("order_count"),
            F.col("gross_total").alias("gross_total"),
            F.col("net_total").alias("net_total"),
        )
        assert_schema(df, CUSTOMER_DAILY_TOTAL_SCHEMA, name="CustomerDailyTotal", mode="strict")
        return df

    def product_daily_summary(self, *, fulfilled: DataFrame) -> DataFrame:
        assert_schema(fulfilled, ORDER_FULFILLMENT_SCHEMA, name="OrderFulfillment", mode="strict")

        df = fulfilled.groupBy(
            F.col("tenant.tenant_id").alias("tenant_id"),
            F.col("product_id").alias("product_id"),
            F.col("business.order_date").alias("order_date"),
        ).agg(
            F.first(F.col("tenant"), ignorenulls=False).alias("tenant"),
            F.count(F.lit(1)).cast("long").alias("order_count"),
            F.sum(F.col("quantity")).cast("long").alias("units"),
            F.sum(F.col("total")).cast("decimal(12,2)").alias("gross_total"),
        ).select(
            F.col("tenant").alias("tenant"),
            F.col("product_id").alias("product_id"),
            F.col("order_date").alias("order_date"),
            F.col("order_count").alias("order_count"),
            F.col("units").alias("units"),
            F.col("gross_total").alias("gross_total"),
        )
        assert_schema(df, PRODUCT_DAILY_SUMMARY_SCHEMA, name="ProductDailySummary", mode="strict")
        return df


def order_customer_daily_totals(
    *,
    fulfilled: DataFrame,
    spark: SparkSession,
    ctx=None,
) -> DataFrame:
    return OrderAnalyticsGenerated(spark=spark, ctx=ctx).customer_daily_totals(fulfilled=fulfilled)


def order_product_daily_summary(
    *,
    fulfilled: DataFrame,
    spark: SparkSession,
    ctx=None,
) -> DataFrame:
    return OrderAnalyticsGenerated(spark=spark, ctx=ctx).product_daily_summary(fulfilled=fulfilled)
