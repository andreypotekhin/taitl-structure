from typing import Any, cast

import pytest

import structure
from structure.app.dsl.api import compile_transform
from structure.app.target.capabilities.api import BackendCapabilityError, PySparkCapabilities
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
    class Raw(structure.Schema):
        promotion_code = structure.field(structure.String(), nullable=True, alias="promo-code")

    class Published(structure.Schema):
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
    class Address(structure.Schema):
        city = structure.field(structure.String(), nullable=False)
        postal_code = structure.field(structure.String(), nullable=False)

    class Raw(structure.Schema):
        id = structure.field(structure.String(), nullable=False)
        shipping = structure.field(structure.Struct(Address), nullable=True)

    class Published(structure.Schema):
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
    class Address(structure.Schema):
        postal_code = structure.field(structure.String(), nullable=False, alias="postal.code")

    class Raw(structure.Schema):
        shipping = structure.field(structure.Struct(Address), nullable=False)

    class Published(structure.Schema):
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
    class Raw(structure.Schema):
        customer_id = structure.field(structure.String(), nullable=False)
        status = structure.field(structure.String(), nullable=True)
        total = structure.field(structure.Integer(), nullable=False)
        tax = structure.field(structure.Integer(), nullable=False)
        price = structure.field(structure.Integer(), nullable=False)
        quantity = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
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


def test_v3_expression_renderer_renders_string_predicates() -> None:
    class Raw(structure.Schema):
        status = structure.field(structure.String(), nullable=True)

    class Published(structure.Schema):
        contains_new = structure.field(structure.Boolean(), nullable=True)
        matches_new = structure.field(structure.Boolean(), nullable=True)
        matches_new_case_insensitive = structure.field(structure.Boolean(), nullable=True)
        matches_release = structure.field(structure.Boolean(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            status = cast(Any, row).status
            return Published(
                contains_new=status.contains("new"),
                matches_new=status.like("new%"),
                matches_new_case_insensitive=status.ilike("NEW%"),
                matches_release=status.rlike(r"release-[0-9]+"),
            )

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.status").contains(\'new\')',
        'F.col("orders.status").like(\'new%\')',
        'F.col("orders.status").ilike(\'NEW%\')',
        "F.col(\"orders.status\").rlike('release-[0-9]+')",
    ]


def test_v3_expression_renderer_renders_collection_indexing() -> None:
    class Raw(structure.Schema):
        tags = structure.field(structure.Array(structure.String(), contains_null=False), nullable=False)
        attributes = structure.field(
            structure.Map(structure.String(), structure.String(), value_contains_null=False), nullable=False
        )

    class Published(structure.Schema):
        first_tag = structure.field(structure.String(), nullable=True)
        region = structure.field(structure.String(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            source = cast(Any, row)
            return Published(first_tag=source.tags[0], region=source.attributes["region"])

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.tags")[0]',
        'F.col("orders.attributes")[\'region\']',
    ]


def test_v3_expression_renderer_renders_scalar_casts() -> None:
    class Raw(structure.Schema):
        raw_count = structure.field(structure.String(), nullable=True)
        count = structure.field(structure.Integer(), nullable=False)

    class Published(structure.Schema):
        count = structure.field(structure.Integer(), nullable=True)
        count_text = structure.field(structure.String(), nullable=False)
        try_count = structure.field(structure.Integer(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            source = cast(Any, row)
            return Published(
                count=source.raw_count.cast(structure.Integer()),
                count_text=source.count.astype(structure.String()),
                try_count=source.raw_count.try_cast(structure.Integer()),
            )

    plan = compile_transform(Publish)
    with pytest.raises(BackendCapabilityError):
        PySpark.plan.lower()(plan)

    recipe = PySpark.plan.lower()(plan, capabilities=PySparkCapabilities(target_profile=">=4.0,<4.1"))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.raw_count").cast(\'int\')',
        'F.col("orders.count").cast(\'string\')',
        'F.col("orders.raw_count").try_cast(\'int\')',
    ]


def test_v3_expression_renderer_renders_string_sql_helpers() -> None:
    class Raw(structure.Schema):
        label = structure.field(structure.String(), nullable=True)

    class Published(structure.Schema):
        prefix = structure.field(structure.String(), nullable=True)
        parts = structure.field(structure.Array(structure.String(), contains_null=False), nullable=True)
        normalized = structure.field(structure.String(), nullable=True)
        extracted = structure.field(structure.String(), nullable=True)
        character_count = structure.field(structure.Integer(), nullable=True)
        title = structure.field(structure.String(), nullable=True)
        backward = structure.field(structure.String(), nullable=True)
        normalized_letters = structure.field(structure.String(), nullable=True)
        dash_position = structure.field(structure.Integer(), nullable=True)
        distance = structure.field(structure.Integer(), nullable=True)
        joined = structure.field(structure.String(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                prefix=structure.substring(row.label, start=1, length=3),
                parts=structure.split(row.label, pattern="-"),
                normalized=structure.regexp_replace(row.label, pattern=r"\s+", replacement=" "),
                extracted=structure.regexp_extract(row.label, pattern=r"^([^-]+)", group=1),
                character_count=structure.length(row.label),
                title=structure.initcap(row.label),
                backward=structure.reverse(row.label),
                normalized_letters=structure.translate(row.label, matching="-", replacement="_"),
                dash_position=structure.instr(row.label, substring="-"),
                distance=structure.levenshtein(row.label, "release"),
                joined=structure.concat_ws(" / ", row.label, "release"),
            )

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.substring(F.col("orders.label"), 1, 3)',
        'F.split(F.col("orders.label"), \'-\', -1)',
        "F.regexp_replace(F.col(\"orders.label\"), '\\\\s+', ' ')",
        "F.regexp_extract(F.col(\"orders.label\"), '^([^-]+)', 1)",
        'F.length(F.col("orders.label"))',
        'F.initcap(F.col("orders.label"))',
        'F.reverse(F.col("orders.label"))',
        'F.translate(F.col("orders.label"), \'-\', \'_\')',
        'F.instr(F.col("orders.label"), \'-\')',
        'F.levenshtein(F.col("orders.label"), F.lit(\'release\'))',
        'F.concat_ws(\' / \', F.col("orders.label"), F.lit(\'release\'))',
    ]


def test_v3_expression_renderer_renders_temporal_sql_helpers() -> None:
    class Raw(structure.Schema):
        start_date = structure.field(structure.Date(), nullable=False)
        end_date = structure.field(structure.Date(), nullable=True)
        recorded_at = structure.field(structure.Timestamp(), nullable=True)

    class Published(structure.Schema):
        due_date = structure.field(structure.Date(), nullable=False)
        elapsed_days = structure.field(structure.Integer(), nullable=True)
        month = structure.field(structure.Timestamp(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                due_date=structure.date_add(row.start_date, days=7),
                elapsed_days=structure.datediff(row.end_date, row.start_date),
                month=structure.date_trunc(row.recorded_at, unit="month"),
            )

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.date_add(F.col("orders.start_date"), 7)',
        'F.datediff(F.col("orders.end_date"), F.col("orders.start_date"))',
        'F.date_trunc(\'month\', F.col("orders.recorded_at"))',
    ]


def test_v3_expression_renderer_renders_numeric_sql_helpers() -> None:
    class Raw(structure.Schema):
        amount = structure.field(structure.Decimal(precision=12, scale=2), nullable=True)

    class Published(structure.Schema):
        absolute_amount = structure.field(structure.Decimal(precision=12, scale=2), nullable=True)
        rounded_amount = structure.field(structure.Decimal(precision=12, scale=2), nullable=True)
        ceiling = structure.field(structure.Decimal(precision=11, scale=0), nullable=True)
        floor = structure.field(structure.Decimal(precision=11, scale=0), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                absolute_amount=structure.abs(row.amount),
                rounded_amount=structure.round(row.amount, scale=1),
                ceiling=structure.ceil(row.amount),
                floor=structure.floor(row.amount),
            )

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.abs(F.col("orders.amount"))',
        'F.round(F.col("orders.amount"), 1)',
        'F.ceil(F.col("orders.amount"))',
        'F.floor(F.col("orders.amount"))',
    ]


def test_v3_expression_renderer_renders_predicate_sql_helpers() -> None:
    class Raw(structure.Schema):
        label = structure.field(structure.String(), nullable=True)
        score = structure.field(structure.Double(), nullable=True)

    class Published(structure.Schema):
        missing_label = structure.field(structure.Boolean(), nullable=False)
        present_label = structure.field(structure.Boolean(), nullable=False)
        invalid_score = structure.field(structure.Boolean(), nullable=False)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                missing_label=structure.isnull(row.label),
                present_label=structure.isnotnull(row.label),
                invalid_score=structure.isnan(row.score),
            )

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.label").isNull()',
        'F.col("orders.label").isNotNull()',
        'F.isnan(F.col("orders.score"))',
    ]


def test_v3_expression_renderer_renders_struct_get_field() -> None:
    class Address(structure.Schema):
        city = structure.field(structure.String(), nullable=False, alias="city-name")

    class Raw(structure.Schema):
        address = structure.field(structure.Struct(Address), nullable=True)

    class Published(structure.Schema):
        city = structure.field(structure.String(), nullable=True)

    @structure.transform
    class Publish(structure.Transform):
        rows = structure.input(Raw)
        published = structure.output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(city=cast(Any, row).address.get_field("city"))

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "orders"}) == (
        'F.col("orders.address").getField(\'city-name\')'
    )
