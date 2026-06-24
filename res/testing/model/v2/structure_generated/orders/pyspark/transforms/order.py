from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from testing.model.v2.orders.transforms.order import EnrichOrders
from testing.model.v2.structure_generated.orders.pyspark.schemas.customer import CUSTOMER_SCHEMA
from testing.model.v2.structure_generated.orders.pyspark.schemas.order import (
    ORDER_FULFILLMENT_SCHEMA,
    ORDER_NORMALIZED_SCHEMA,
    ORDER_PUBLISHED_SCHEMA,
    ORDER_RAW_SCHEMA,
    ORDER_WITH_CUSTOMER_SCHEMA,
    ORDER_WITH_PRODUCT_SCHEMA,
    ORDER_WITH_PROMOTION_SCHEMA,
)
from testing.model.v2.structure_generated.orders.pyspark.schemas.product import PRODUCT_SCHEMA
from testing.model.v2.structure_generated.orders.pyspark.schemas.promotion import PROMOTION_SCHEMA
from testing.model.v2.structure_generated.orders.pyspark.schemas.shipment import SHIPMENT_SCHEMA
from testing.model.v2.structure_generated.runtime.hook_inputs import HookInputs
from testing.model.v2.structure_generated.runtime.schema_assert import assert_schema, project_schema


class EnrichOrdersGenerated:

    def __init__(self, *, spark: SparkSession, ctx=None):
        self.spark = spark
        self.ctx = ctx
        self._impl = EnrichOrders()

    def run(
        self,
        *,
        orders: DataFrame,
        customers: DataFrame,
        products: DataFrame,
        promotions: DataFrame,
        shipments: DataFrame,
    ) -> DataFrame:
        assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")
        assert_schema(customers, CUSTOMER_SCHEMA, name="Customer", mode="strict")
        assert_schema(products, PRODUCT_SCHEMA, name="Product", mode="strict")
        assert_schema(promotions, PROMOTION_SCHEMA, name="Promotion", mode="strict")
        assert_schema(shipments, SHIPMENT_SCHEMA, name="Shipment", mode="strict")

        inputs = HookInputs(
            orders=orders,
            customers=customers,
            products=products,
            promotions=promotions,
            shipments=shipments,
        )

        # Hook before normalize: use_current_orders
        orders = self._impl.use_current_orders(orders=orders, inputs=inputs, spark=self.spark, ctx=self.ctx)

        # Subtransform: normalize
        total = F.coalesce(F.col("total").cast("decimal(12,2)"), F.lit(0).cast("decimal(12,2)"))
        discount = F.coalesce(F.col("discount").cast("decimal(12,2)"), F.lit(0).cast("decimal(12,2)"))
        clean_tags = F.filter(
            F.transform(F.col("tags"), lambda tag: F.lower(F.trim(tag))),
            lambda tag: tag.isNotNull(),
        )
        orders = orders.where(
            F.col("id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("product_id").isNotNull()
        ).select(
            F.col("tenant").alias("tenant"),
            F.col("audit").alias("audit"),
            F.col("business").alias("business"),
            F.lower(F.trim(F.col("id"))).alias("id"),
            F.lower(F.trim(F.col("customer_id"))).alias("customer_id"),
            F.lower(F.trim(F.col("product_id"))).alias("product_id"),
            F.lower(F.trim(F.col("promotion_code"))).alias("promotion_code"),
            total.alias("total"),
            discount.alias("discount"),
            (total - discount).alias("net_total"),
            F.coalesce(F.col("quantity").cast("long"), F.lit(1).cast("long")).alias("quantity"),
            clean_tags.alias("tags"),
            F.col("attributes").alias("attributes"),
            F.col("shipping").alias("shipping"),
            (total > F.lit(1000).cast("decimal(12,2)")).alias("is_large"),
        )

        orders = self._impl.remove_negative_totals(orders=orders, spark=self.spark, ctx=self.ctx)
        assert_schema(orders, ORDER_NORMALIZED_SCHEMA, name="OrderNormalized", mode="strict")

        # Subtransform: add_customer
        orders = orders.alias("order_normalized")
        customers_joined = F.broadcast(customers.alias("customers"))
        orders = orders.join(
            customers_joined,
            (F.col("customers.tenant.tenant_id") == F.col("order_normalized.tenant.tenant_id"))
            & (F.lower(F.trim(F.col("customers.id"))) == F.col("order_normalized.customer_id")),
            "left",
        ).select(
            F.col("order_normalized.tenant").alias("tenant"),
            F.col("order_normalized.audit").alias("audit"),
            F.col("order_normalized.business").alias("business"),
            F.col("order_normalized.id").alias("id"),
            F.col("order_normalized.customer_id").alias("customer_id"),
            F.col("order_normalized.product_id").alias("product_id"),
            F.col("order_normalized.promotion_code").alias("promotion_code"),
            F.col("order_normalized.total").alias("total"),
            F.col("order_normalized.discount").alias("discount"),
            F.col("order_normalized.net_total").alias("net_total"),
            F.col("order_normalized.quantity").alias("quantity"),
            F.col("order_normalized.tags").alias("tags"),
            F.col("order_normalized.attributes").alias("attributes"),
            F.col("order_normalized.shipping").alias("shipping"),
            F.col("order_normalized.is_large").alias("is_large"),
            F.col("customers.name").alias("customer_name"),
            F.col("customers.tier").alias("customer_tier"),
            F.col("customers.region").alias("customer_region"),
        )
        assert_schema(orders, ORDER_WITH_CUSTOMER_SCHEMA, name="OrderWithCustomer", mode="strict")
        orders = orders.persist(StorageLevel.MEMORY_AND_DISK)

        # Subtransform: add_product
        orders = orders.alias("order_with_customer")
        products_joined = products.alias("products")
        orders = orders.join(
            products_joined,
            (F.col("products.tenant.tenant_id") == F.col("order_with_customer.tenant.tenant_id"))
            & (F.col("products.id") == F.col("order_with_customer.product_id")),
            "left",
        ).where(
            F.col("products.id").isNotNull()
        ).select(
            F.col("order_with_customer.tenant").alias("tenant"),
            F.col("order_with_customer.audit").alias("audit"),
            F.col("order_with_customer.business").alias("business"),
            F.col("order_with_customer.id").alias("id"),
            F.col("order_with_customer.customer_id").alias("customer_id"),
            F.col("order_with_customer.product_id").alias("product_id"),
            F.col("order_with_customer.promotion_code").alias("promotion_code"),
            F.col("order_with_customer.total").alias("total"),
            F.col("order_with_customer.discount").alias("discount"),
            F.col("order_with_customer.net_total").alias("net_total"),
            F.col("order_with_customer.quantity").alias("quantity"),
            F.col("order_with_customer.tags").alias("tags"),
            F.col("order_with_customer.attributes").alias("attributes"),
            F.col("order_with_customer.shipping").alias("shipping"),
            F.col("order_with_customer.is_large").alias("is_large"),
            F.col("order_with_customer.customer_name").alias("customer_name"),
            F.col("order_with_customer.customer_tier").alias("customer_tier"),
            F.col("order_with_customer.customer_region").alias("customer_region"),
            F.col("products.name").alias("product_name"),
            F.col("products.category").alias("product_category"),
            F.col("products.active").alias("product_active"),
            F.col("products.list_price").alias("product_list_price"),
        )
        assert_schema(orders, ORDER_WITH_PRODUCT_SCHEMA, name="OrderWithProduct", mode="strict")

        # Subtransform: add_promotion
        orders = orders.alias("order_with_product")
        promotions_joined = promotions.alias("promotions")
        orders = orders.join(
            promotions_joined,
            (F.col("promotions.tenant.tenant_id") == F.col("order_with_product.tenant.tenant_id"))
            & F.lower(F.trim(F.col("promotions.code"))).eqNullSafe(F.col("order_with_product.promotion_code")),
            "left",
        ).select(
            F.col("order_with_product.tenant").alias("tenant"),
            F.col("order_with_product.audit").alias("audit"),
            F.col("order_with_product.business").alias("business"),
            F.col("order_with_product.id").alias("id"),
            F.col("order_with_product.customer_id").alias("customer_id"),
            F.col("order_with_product.product_id").alias("product_id"),
            F.col("order_with_product.promotion_code").alias("promotion_code"),
            F.col("order_with_product.total").alias("total"),
            F.col("order_with_product.discount").alias("discount"),
            F.col("order_with_product.net_total").alias("net_total"),
            F.col("order_with_product.quantity").alias("quantity"),
            F.col("order_with_product.tags").alias("tags"),
            F.col("order_with_product.attributes").alias("attributes"),
            F.col("order_with_product.shipping").alias("shipping"),
            F.col("order_with_product.is_large").alias("is_large"),
            F.col("order_with_product.customer_name").alias("customer_name"),
            F.col("order_with_product.customer_tier").alias("customer_tier"),
            F.col("order_with_product.customer_region").alias("customer_region"),
            F.col("order_with_product.product_name").alias("product_name"),
            F.col("order_with_product.product_category").alias("product_category"),
            F.col("order_with_product.product_active").alias("product_active"),
            F.col("order_with_product.product_list_price").alias("product_list_price"),
            F.col("promotions.name").alias("promotion_name"),
            F.col("promotions.discount").alias("promotion_discount"),
        )
        assert_schema(orders, ORDER_WITH_PROMOTION_SCHEMA, name="OrderWithPromotion", mode="strict")

        # Subtransform: add_shipments
        orders = orders.alias("order_with_promotion")
        shipments_joined = shipments.hint("shuffle_hash").alias("shipments")
        orders = orders.join(
            shipments_joined,
            (F.col("shipments.tenant.tenant_id") == F.col("order_with_promotion.tenant.tenant_id"))
            & (F.col("shipments.order_id") == F.col("order_with_promotion.id")),
            "inner",
        ).select(
            F.col("order_with_promotion.tenant").alias("tenant"),
            F.col("order_with_promotion.audit").alias("audit"),
            F.col("order_with_promotion.business").alias("business"),
            F.col("order_with_promotion.id").alias("id"),
            F.col("order_with_promotion.customer_id").alias("customer_id"),
            F.col("order_with_promotion.product_id").alias("product_id"),
            F.col("order_with_promotion.promotion_code").alias("promotion_code"),
            F.col("order_with_promotion.total").alias("total"),
            F.col("order_with_promotion.discount").alias("discount"),
            F.col("order_with_promotion.net_total").alias("net_total"),
            F.col("order_with_promotion.quantity").alias("quantity"),
            F.col("order_with_promotion.tags").alias("tags"),
            F.col("order_with_promotion.attributes").alias("attributes"),
            F.col("order_with_promotion.shipping").alias("shipping"),
            F.col("order_with_promotion.is_large").alias("is_large"),
            F.col("order_with_promotion.customer_name").alias("customer_name"),
            F.col("order_with_promotion.customer_tier").alias("customer_tier"),
            F.col("order_with_promotion.customer_region").alias("customer_region"),
            F.col("order_with_promotion.product_name").alias("product_name"),
            F.col("order_with_promotion.product_category").alias("product_category"),
            F.col("order_with_promotion.product_active").alias("product_active"),
            F.col("order_with_promotion.product_list_price").alias("product_list_price"),
            F.col("order_with_promotion.promotion_name").alias("promotion_name"),
            F.col("order_with_promotion.promotion_discount").alias("promotion_discount"),
            F.col("shipments.line_number").alias("shipment_line"),
            F.col("shipments.carrier").alias("carrier"),
            F.col("shipments.tracking_number").alias("tracking_number"),
            F.col("shipments.shipped_at").alias("shipped_at"),
        )
        assert_schema(orders, ORDER_FULFILLMENT_SCHEMA, name="OrderFulfillment", mode="strict")

        orders = self._impl.note_lookup_inputs(orders=orders, inputs=inputs, spark=self.spark, ctx=self.ctx)
        assert_schema(orders, ORDER_FULFILLMENT_SCHEMA, name="OrderFulfillment", mode="allow_extra_columns")

        # Subtransform: publish
        published = orders.alias("order_fulfillment")
        published = published.select(
            F.col("order_fulfillment.tenant").alias("tenant"),
            F.col("order_fulfillment.business").alias("business"),
            F.col("order_fulfillment.id").alias("id"),
            F.col("order_fulfillment.customer_id").alias("customer_id"),
            F.col("order_fulfillment.customer_name").alias("customer_name"),
            F.col("order_fulfillment.customer_tier").alias("customer_tier"),
            F.col("order_fulfillment.product_name").alias("product_name"),
            F.col("order_fulfillment.product_category").alias("product_category"),
            F.col("order_fulfillment.promotion_name").alias("promotion_name"),
            F.col("order_fulfillment.total").alias("total"),
            F.col("order_fulfillment.discount").alias("discount"),
            F.col("order_fulfillment.net_total").alias("net_total"),
            F.col("order_fulfillment.quantity").alias("quantity"),
            F.col("order_fulfillment.carrier").alias("carrier"),
            F.col("order_fulfillment.tracking_number").alias("tracking_number"),
            F.col("order_fulfillment.shipped_at").alias("shipped_at"),
            F.col("order_fulfillment.is_large").alias("is_large"),
            F.col("order_fulfillment.promotion_name").isNotNull().alias("has_promotion"),
        )

        published = self._impl.add_quality_columns(published=published, spark=self.spark, ctx=self.ctx)
        assert_schema(published, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="allow_extra_columns")
        published = project_schema(published, ORDER_PUBLISHED_SCHEMA)
        assert_schema(published, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="strict")
        return published


def enrich_orders(
    *,
    orders: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    promotions: DataFrame,
    shipments: DataFrame,
    spark: SparkSession,
    ctx=None,
) -> DataFrame:
    return EnrichOrdersGenerated(spark=spark, ctx=ctx).run(
        orders=orders,
        customers=customers,
        products=products,
        promotions=promotions,
        shipments=shipments,
    )
