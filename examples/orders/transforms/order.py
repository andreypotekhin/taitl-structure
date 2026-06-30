from examples.orders.schemas.customer import Customer
from examples.orders.schemas.order import (
    OrderNormalized,
    OrderPublished,
    OrderRaw,
    OrderWithCustomer,
    OrderWithProduct,
    OrderWithPromotion,
    PublicationFlags,
)
from examples.orders.schemas.product import Product
from examples.orders.schemas.promotion import Promotion
from structure import (
    Join,
    JoinHint,
    SchemaMode,
    Transform,
    after,
    before,
    coalesce,
    expr_fn,
    input,
    join_one,
    lower,
    output,
    to_decimal,
    transform,
    trim,
    where,
)


@transform(streaming_compatible=True)
class EnrichOrders(Transform):
    orders = input(OrderRaw)
    customers = input(Customer)
    products = input(Product)
    promotions = input(Promotion)
    published = output(OrderPublished)

    @expr_fn
    def clean_id(value):
        return lower(trim(value))

    @expr_fn
    def money(value):
        return coalesce(to_decimal(value, precision=12, scale=2), 0)

    def normalize(self, order: OrderRaw) -> OrderNormalized:
        where(order.id.is_not_null())
        where(order.customer_id.is_not_null())
        where(order.product_id.is_not_null())

        total = self.money(order.total)
        discount = self.money(order.discount)

        return OrderNormalized(
            tenant=order.tenant,
            audit=order.audit,
            business=order.business,
            id=self.clean_id(order.id),
            customer_id=self.clean_id(order.customer_id),
            product_id=self.clean_id(order.product_id),
            promotion_code=self.clean_id(order.promotion_code),
            total=total,
            discount=discount,
            net_total=total - discount,
            quantity=coalesce(order.quantity, 1),
            tags=order.tags,
            attributes=order.attributes,
            shipping=order.shipping,
            is_large=total > 1000,
        )

    @before(normalize, lane=orders, pass_inputs=True, streaming_safe=True)
    def use_current_orders(self, *, orders, inputs, spark, ctx):
        if ctx is not None and getattr(ctx, "use_original_orders", False):
            return inputs.orders
        return orders

    @after(normalize, lane=orders, streaming_safe=True)
    def remove_negative_totals(self, *, orders, spark, ctx):
        from pyspark.sql import functions as F

        return orders.where(F.col("net_total") >= 0)

    def add_customer(self, order: OrderNormalized) -> OrderWithCustomer:
        customer = join_one(
            on=(self.customers.tenant.tenant_id == order.tenant.tenant_id)
            & (self.clean_id(self.customers.id) == order.customer_id),
            how=Join.LEFT,
            hint=JoinHint.BROADCAST,
        )

        return OrderWithCustomer.base(order)(
            customer_name=customer.name,
            customer_tier=customer.tier,
            customer_region=customer.region,
        )

    def add_product(self, order: OrderWithCustomer, product: Product) -> OrderWithProduct:
        join_one(
            on=(product.tenant.tenant_id == order.tenant.tenant_id) & (product.id == order.product_id),
            how=Join.LEFT,
        )

        where(product.id.is_not_null())

        return OrderWithProduct.base(order)(
            product_name=product.name,
            product_category=product.category,
            product_active=product.active,
            product_list_price=product.list_price,
        )

    def add_promotion(self, order: OrderWithProduct) -> OrderWithPromotion:
        join_one(
            on=(self.promotions.tenant.tenant_id == order.tenant.tenant_id)
            & self.clean_id(self.promotions.code).null_safe_eq(order.promotion_code),
            how=Join.LEFT,
        )

        return OrderWithPromotion.base(order)(
            promotion_name=self.promotions.name,
            promotion_discount=self.promotions.discount,
        )

    @after(
        add_promotion,
        lane=orders,
        pass_inputs=True,
        schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS,
        project_output=True,
        streaming_safe=True,
    )
    def note_lookup_inputs(self, *, orders, inputs, spark, ctx):
        from pyspark.sql import functions as F

        return orders.withColumn(
            "_lookup_inputs_seen", F.lit(inputs.customers is not None and inputs.products is not None)
        )

    @transform(output=published)
    def publish(self, order: OrderWithPromotion) -> OrderPublished:
        flags = PublicationFlags(
            has_promotion=order.promotion_name.is_not_null(),
        )

        return OrderPublished.base(order, flags)

    @after(
        publish, lane=published, schema_mode=SchemaMode.ALLOW_EXTRA_COLUMNS, project_output=True, streaming_safe=True
    )
    def add_quality_columns(self, *, published, spark, ctx):
        from pyspark.sql import functions as F

        return published.withColumn("_has_customer", F.col("customer_name").isNotNull())
