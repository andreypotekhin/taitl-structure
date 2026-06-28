from structure import String, Structure, Transform, field, input, output, transform
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


def test_v1_expression_renderer_renders_filter_helpers_and_literals() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = PySpark.plan.lower()(compile_transform(EnrichOrders))
    normalize = recipe.steps[0]

    assert PySpark.render.expression()(normalize.filters[0], scope_aliases={"orders": "orders"}) == (
        'F.col("orders.id").isNotNull()'
    )

    projection = {assignment.field.name: assignment.expression for assignment in normalize.projection}
    assert PySpark.render.expression()(projection["id"], scope_aliases={"orders": "orders"}) == (
        'F.lower(F.trim(F.col("orders.id")))'
    )
    assert PySpark.render.expression()(projection["total"], scope_aliases={"orders": "orders"}) == (
        'F.coalesce(F.col("orders.total").cast("decimal(12,2)"), F.lit(0))'
    )


def test_v1_expression_renderer_renders_arithmetic_and_comparison() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = PySpark.plan.lower()(compile_transform(EnrichOrders))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}

    assert PySpark.render.expression()(projection["net_total"], scope_aliases={"orders": "orders"}) == (
        '(F.coalesce(F.col("orders.total").cast("decimal(12,2)"), F.lit(0)) - '
        'F.coalesce(F.col("orders.discount").cast("decimal(12,2)"), F.lit(0)))'
    )
    assert PySpark.render.expression()(projection["is_large"], scope_aliases={"orders": "orders"}) == (
        '(F.coalesce(F.col("orders.total").cast("decimal(12,2)"), F.lit(0)) > F.lit(1000))'
    )


def test_v1_expression_renderer_renders_join_predicates() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = PySpark.plan.lower()(compile_transform(EnrichOrders))
    customer_join = recipe.steps[1].joins[0]
    promotion_join = recipe.steps[3].joins[0]

    assert PySpark.render.expression()(
        customer_join.predicate,
        scope_aliases={"customers": "customers", "OrderNormalized": "order_normalized"},
    ) == (
        '((F.col("customers.tenant.tenant_id") == F.col("order_normalized.tenant.tenant_id")) & '
        '(F.lower(F.trim(F.col("customers.id"))) == F.col("order_normalized.customer_id")))'
    )
    assert PySpark.render.expression()(
        promotion_join.predicate,
        scope_aliases={"promotions": "promotions", "OrderWithProduct": "order_with_product"},
    ) == (
        '((F.col("promotions.tenant.tenant_id") == F.col("order_with_product.tenant.tenant_id")) & '
        'F.lower(F.trim(F.col("promotions.code"))).eqNullSafe(F.col("order_with_product.promotion_code")))'
    )


def test_v1_expression_renderer_passes_field_aliases_to_spark() -> None:
    class Raw(Structure):
        promotion_code = field(String(), nullable=True, alias="promo-code")

    class Published(Structure):
        promotion_code = field(String(), nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(promotion_code=row.promotion_code)

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "rows"}) == 'F.col("rows.promo-code")'
