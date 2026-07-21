from pyspark import StorageLevel
from pyspark.sql import functions as F
from testing.model.v3.orders.schemas.customer import Customer
from testing.model.v3.orders.schemas.order import (
    OrderFulfillment,
    OrderNormalized,
    OrderPublished,
    OrderRaw,
    OrderWithCustomer,
    OrderWithProduct,
    OrderWithPromotion,
    PublicationFlags,
)
from testing.model.v3.orders.schemas.product import BlockedProduct, Product
from testing.model.v3.orders.schemas.promotion import Promotion
from testing.model.v3.orders.schemas.shipment import Shipment

from structure import *
from structure.plugin.pyspark import *


@transform(streaming_compatible=True)
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)
    products = input(Product)
    blocked_products = input(BlockedProduct)
    promotions = input(Promotion)
    shipments = input(Shipment)
    published = output(OrderPublished)

    @special(type="expr")
    def clean_id(value):
        return lower(trim(value))

    @special(type="expr")
    def money(value):
        return coalesce(to_decimal(value, precision=12, scale=2), 0)

    @raw(inout=input(orders) | lane(orders), streaming_safe=True)
    def use_current_orders(self, *, orders, spark, ctx):
        return orders

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        where(order.customer_id.is_not_null())
        where(order.product_id.is_not_null())

        total = self.money(order.total)
        discount = self.money(order.discount)

        return OrderNormalized.project(order)(
            id=self.clean_id(order.id),
            customer_id=self.clean_id(order.customer_id),
            product_id=self.clean_id(order.product_id),
            promotion_code=self.clean_id(order.promotion_code),
            total=total,
            discount=discount,
            net_total=(total - discount).cast(types.decimal(12, 2)),
            quantity=coalesce(order.quantity, 1),
            tags=arr_filter(
                arr_transform(order.tags, lambda tag: lower(trim(tag))),
                lambda tag: tag.is_not_null(),
            ),
            attributes=map_filter(
                map_transform_values(
                    order.attributes, lambda key, value: lower(trim(value))
                ),
                lambda key, value: value.is_not_null(),
            ),
            shipping=order.shipping,
            is_large=total > 1000,
        )

    @raw(streaming_safe=True)
    def remove_negative_totals(self, *, orders, spark, ctx):
        return orders.where(F.col("net_total") >= 0)

    @step(cache=StorageLevel.MEMORY_AND_DISK)
    def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
        customer = left_join(
            customer,
            on=(customer.tenant.tenant_id == order.tenant.tenant_id)
            & (self.clean_id(customer.id) == order.customer_id),
            hint=JoinHint.BROADCAST,
        )

        return OrderWithCustomer.base(order)(
            customer_name=customer.name,
            customer_tier=customer.tier,
            customer_region=customer.region,
        )

    def add_product(
        self, order: OrderWithCustomer, product: Product, blocked_product: BlockedProduct
    ) -> OrderWithProduct:
        where(
            exists(on=(product.tenant.tenant_id == order.tenant.tenant_id) & (product.id == order.product_id))
        )
        where(
            not_exists(
                on=(blocked_product.tenant.tenant_id == order.tenant.tenant_id)
                & (blocked_product.id == order.product_id)
            )
        )
        lookup_join(
            on=(product.tenant.tenant_id == order.tenant.tenant_id) & (product.id == order.product_id),
            how=Join.LEFT,
            dedupe=JoinDedupe.latest_by(product.audit.ingested_at, ties=TiePolicy.ERROR),
        )

        where(product.id.is_not_null())

        return OrderWithProduct.base(order)(
            product_name=product.name,
            product_category=product.category,
            product_active=product.active,
            product_list_price=product.list_price,
        )

    def add_promotion(self, order: OrderWithProduct, promotion: Promotion) -> OrderWithPromotion:
        temporal_one(
            promotion,
            on=(promotion.tenant.tenant_id == order.tenant.tenant_id)
            & self.clean_id(promotion.code).null_safe_eq(order.promotion_code),
            at=order.business.order_date,
            valid_from=promotion.valid_from,
            valid_to=promotion.valid_to,
            how=Join.LEFT,
        )

        return OrderWithPromotion.base(order)(
            promotion_name=promotion.name,
            promotion_discount=promotion.discount,
        )

    def add_shipments(self, order: OrderWithPromotion, shipment: Shipment) -> OrderFulfillment:
        inner_join(
            shipment,
            on=(shipment.tenant.tenant_id == order.tenant.tenant_id) & (shipment.order_id == order.id),
            strategy=JoinStrategy.SHUFFLE_HASH,
        )

        return OrderFulfillment.base(order)(
            shipment_line=shipment.line_number,
            carrier=shipment.carrier,
            tracking_number=shipment.tracking_number,
            shipped_at=shipment.shipped_at,
        )

    @raw(
        inout=[lane(orders), input(customers), input(products)] | lane(orders),
        schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS,
        streaming_safe=True,
    )
    def note_lookup_inputs(self, *, orders, customers, products, spark, ctx):
        return orders.withColumn(
            "_lookup_inputs_seen", F.lit(customers is not None and products is not None)
        )

    @step(output=published)
    def publish(self, order: OrderFulfillment) -> OrderPublished:
        flags = PublicationFlags(
            has_promotion=order.promotion_name.is_not_null(),
        )

        return OrderPublished.base(order, flags)

    @raw(
        inout=lane(published) | output(published),
        schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS,
        project_output=True,
        streaming_safe=True,
    )
    def add_quality_columns(self, *, published, spark, ctx):
        return published.withColumn("_has_customer", F.col("customer_name").isNotNull()).withColumn(
            "_has_tracking", F.col("tracking_number").isNotNull()
        )
