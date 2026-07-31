from examples.store.schemas.customer import Customer
from examples.store.schemas.fulfillment.demand import Order
from examples.store.schemas.order import (
    OrderNormalized,
    OrderRaw,
    OrderWithCustomer,
    OrderWithProduct,
    OrderWithPromotion,
)
from examples.store.schemas.product import BlockedProduct, Product
from examples.store.schemas.promotion import Promotion
from structure import *
from structure.plugin.pyspark import *


@transform(streaming=True)
class PrepareOrderDemand(Transform):
    orders = input(OrderRaw, streaming=True)
    customers = input(Customer)
    products = input(Product)
    blocked_products = input(BlockedProduct)
    promotions = input(Promotion)
    demand = output(Order)

    @special(type="expr")
    def clean_id(value):
        return lower(trim(value))

    @special(type="expr")
    def money(value):
        return coalesce(to_decimal(value, precision=12, scale=2), 0)

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
                map_transform_values(order.attributes, lambda key, value: lower(trim(value))),
                lambda key, value: value.is_not_null(),
            ),
            shipping=order.shipping,
            is_large=total > 1000,
        )

    def discard_negative_totals(self, order: OrderNormalized) -> OrderNormalized:
        where(order.net_total >= 0)
        return OrderNormalized.project(order)

    def add_customer(self, order: OrderNormalized, customer: Customer) -> OrderWithCustomer:
        left_join(
            on=(customer.tenant.tenant_id == order.tenant.tenant_id)
            & (self.clean_id(customer.id) == order.customer_id),
            hint="broadcast",
        )
        return OrderWithCustomer.project(order)(
            customer_name=customer.name,
            customer_tier=customer.tier,
            customer_region=customer.region,
        )

    def add_product(
        self, order: OrderWithCustomer, product: Product, blocked_product: BlockedProduct
    ) -> OrderWithProduct:
        where(exists(on=(product.tenant.tenant_id == order.tenant.tenant_id) & (product.id == order.product_id)))
        where(
            not_exists(
                on=(blocked_product.tenant.tenant_id == order.tenant.tenant_id)
                & (blocked_product.id == order.product_id)
            )
        )
        lookup_join(
            on=(product.tenant.tenant_id == order.tenant.tenant_id) & (product.id == order.product_id),
            how="left",
            dedupe=JoinDedupe.latest_by(product.audit.ingested_at, ties="error"),
        )
        where(product.id.is_not_null())
        return OrderWithProduct.project(order)(
            product_name=product.name,
            product_category=product.category,
            product_active=product.active,
            product_list_price=product.list_price,
        )

    def add_promotion(self, order: OrderWithProduct, promotion: Promotion) -> OrderWithPromotion:
        temporal_one(
            on=(promotion.tenant.tenant_id == order.tenant.tenant_id)
            & self.clean_id(promotion.code).null_safe_eq(order.promotion_code),
            at=order.business.order_date,
            valid_from=promotion.valid_from,
            valid_to=promotion.valid_to,
            how="left",
        )
        return OrderWithPromotion.project(order)(
            promotion_name=promotion.name,
            promotion_discount=promotion.discount,
        )

    @step(output=demand)
    def publish_demand(self, order: OrderWithPromotion) -> Order:
        return Order.project(order)(
            order_id=order.id,
            requested_quantity=order.quantity,
        )
