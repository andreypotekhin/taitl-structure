from typing import Any, cast

import structure
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
        scope_aliases={"customer": "customers", "order": "order_normalized"},
    ) == (
        '((F.col("customers.tenant.tenant_id") == F.col("order_normalized.tenant.tenant_id")) & '
        '(F.lower(F.trim(F.col("customers.id"))) == F.col("order_normalized.customer_id")))'
    )
    assert PySpark.render.expression()(
        promotion_join.predicate,
        scope_aliases={"promotion": "promotions", "order": "order_with_product"},
    ) == (
        '((F.col("promotions.tenant.tenant_id") == F.col("order_with_product.tenant.tenant_id")) & '
        'F.lower(F.trim(F.col("promotions.code"))).eqNullSafe(F.col("order_with_product.promotion_code")))'
    )


def test_v1_expression_renderer_passes_field_aliases_to_spark() -> None:
    class Raw(structure.Structure):
        promotion_code = structure.field(structure.String(), nullable=True, alias="promo-code")

    class Published(structure.Structure):
        promotion_code = structure.field(structure.String(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(promotion_code=row.promotion_code)

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "rows"}) == 'F.col("rows.promo-code")'


def test_v1_expression_renderer_renders_nested_struct_construction() -> None:
    class Address(structure.Structure):
        city = structure.field(structure.String(), nullable=False)
        postal_code = structure.field(structure.String(), nullable=False)

    class Raw(structure.Structure):
        id = structure.field(structure.String(), nullable=False)
        shipping = structure.field(structure.Struct(Address), nullable=True)

    class Published(structure.Structure):
        id = structure.field(structure.String(), nullable=False)
        shipping = structure.field(structure.Struct(Address), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            structure.where(row.shipping.is_not_null())  # type: ignore[attr-defined]
            return Published(
                id=row.id,
                shipping=Address(
                    city=structure.trim(row.shipping.city),  # type: ignore[attr-defined]
                    postal_code=row.shipping.postal_code,  # type: ignore[attr-defined]
                ),
            )

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}

    assert PySpark.render.expression()(projection["shipping"], scope_aliases={"rows": "rows"}) == (
        'F.struct(F.trim(F.col("rows.shipping.city")).alias("city"), '
        'F.col("rows.shipping.postal_code").alias("postal_code"))'
    )


def test_v1_expression_renderer_escapes_dotted_nested_field_aliases() -> None:
    class Address(structure.Structure):
        postal_code = structure.field(structure.String(), nullable=False, alias="postal.code")

    class Raw(structure.Structure):
        shipping = structure.field(structure.Struct(Address), nullable=False)

    class Published(structure.Structure):
        postal_code = structure.field(structure.String(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(postal_code=row.shipping.postal_code)  # type: ignore[attr-defined]

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "rows"}) == (
        'F.col("rows.shipping.`postal.code`")'
    )


def test_v1_expression_renderer_renders_extended_plain_python_expressions() -> None:
    class Raw(structure.Structure):
        customer_id = structure.field(structure.String(), nullable=False)
        status = structure.field(structure.String(), nullable=True)
        total = structure.field(structure.Integer(), nullable=False)
        tax = structure.field(structure.Integer(), nullable=False)
        price = structure.field(structure.Integer(), nullable=False)
        quantity = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Structure):
        customer_id = structure.field(structure.String(), nullable=False)
        size_tier = structure.field(structure.String(), nullable=False)
        is_big = structure.field(structure.Boolean(), nullable=False)
        is_open = structure.field(structure.Boolean(), nullable=True)
        is_small = structure.field(structure.Boolean(), nullable=False)
        is_at_most_sample = structure.field(structure.Boolean(), nullable=False)
        total_with_tax = structure.field(structure.Integer(), nullable=False)
        line_total = structure.field(structure.Integer(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            order = cast(Any, row)
            return Published(
                customer_id=structure.upper(structure.trim(order.customer_id)),
                size_tier=structure.when(order.total >= 1000, "large").otherwise("standard"),
                is_big=order.total >= 1000,
                is_open=order.status.isin("new", "held"),
                is_small=order.total < 100,
                is_at_most_sample=order.total <= 100,
                total_with_tax=order.total + order.tax,
                line_total=order.price * order.quantity,
            )

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert (
        render(projection["customer_id"], scope_aliases={"rows": "orders"})
        == 'F.upper(F.trim(F.col("orders.customer_id")))'
    )
    assert render(projection["size_tier"], scope_aliases={"rows": "orders"}) == (
        'F.when((F.col("orders.total") >= F.lit(1000)), F.lit(\'large\')).otherwise(F.lit(\'standard\'))'
    )
    assert render(projection["is_big"], scope_aliases={"rows": "orders"}) == '(F.col("orders.total") >= F.lit(1000))'
    assert render(projection["is_open"], scope_aliases={"rows": "orders"}) == (
        'F.col("orders.status").isin(F.lit(\'new\'), F.lit(\'held\'))'
    )
    assert render(projection["is_small"], scope_aliases={"rows": "orders"}) == '(F.col("orders.total") < F.lit(100))'
    assert render(projection["is_at_most_sample"], scope_aliases={"rows": "orders"}) == (
        '(F.col("orders.total") <= F.lit(100))'
    )
    assert render(projection["total_with_tax"], scope_aliases={"rows": "orders"}) == (
        '(F.col("orders.total") + F.col("orders.tax"))'
    )
    assert render(projection["line_total"], scope_aliases={"rows": "orders"}) == (
        '(F.col("orders.price") * F.col("orders.quantity"))'
    )
