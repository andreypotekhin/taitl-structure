from structure import *
from structure.platform.pyspark import *
from testing.model.v1.orders.schemas.customer import Customer
from testing.model.v1.orders.schemas.order import (
    OrderNormalized,
    OrderPublished,
    OrderRaw,
    OrderWithCustomer,
    OrderWithProduct,
    OrderWithPromotion,
    PublicationFlags,
)
from testing.model.v1.orders.schemas.product import Product
from testing.model.v1.orders.schemas.promotion import Promotion


@transform(streaming_compatible=True)
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)
    products = input(Product)
    promotions = input(Promotion)
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
            tags=order.tags,
            attributes=order.attributes,
            shipping=order.shipping,
            is_large=total > 1000,
        )

    @raw(streaming_safe=True)
    def remove_negative_totals(self, *, orders, spark, ctx):
        from pyspark.sql import functions as F

        return orders.where(F.col("net_total") >= 0)

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

    def add_product(self, order: OrderWithCustomer, product: Product) -> OrderWithProduct:
        left_join(
            on=(product.tenant.tenant_id == order.tenant.tenant_id) & (product.id == order.product_id),
        )

        where(product.id.is_not_null())

        return OrderWithProduct.base(order)(
            product_name=product.name,
            product_category=product.category,
            product_active=product.active,
            product_list_price=product.list_price,
        )

    def add_promotion(self, order: OrderWithProduct, promotion: Promotion) -> OrderWithPromotion:
        promotion = left_join(
            promotion,
            on=(promotion.tenant.tenant_id == order.tenant.tenant_id)
            & self.clean_id(promotion.code).null_safe_eq(order.promotion_code),
        )

        return OrderWithPromotion.base(order)(
            promotion_name=promotion.name,
            promotion_discount=promotion.discount,
        )

    @raw(
        inout=[lane(orders), input(customers), input(products)] | lane(orders),
        schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS,
        project_output=True,
        streaming_safe=True,
    )
    def note_lookup_inputs(self, *, orders, customers, products, spark, ctx):
        from pyspark.sql import functions as F

        return orders.withColumn(
            "_lookup_inputs_seen", F.lit(customers is not None and products is not None)
        )

    @step(output=published)
    def publish(self, order: OrderWithPromotion) -> OrderPublished:
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
        from pyspark.sql import functions as F

        return published.withColumn("_has_customer", F.col("customer_name").isNotNull())
