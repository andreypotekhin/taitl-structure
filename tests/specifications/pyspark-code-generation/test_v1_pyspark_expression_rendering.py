from typing import Any, cast

import pytest

from structure import *
from structure.core.target.capabilities.api import BackendCapabilityError
from structure.platform.pyspark import PySpark, field, types
from structure.platform.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities


def test_v1_expression_renderer_renders_filter_helpers_and_literals() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = PySpark.compiler.lower()(compile_transform(EnrichOrders))
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

    recipe = PySpark.compiler.lower()(compile_transform(EnrichOrders))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}

    assert PySpark.render.expression()(projection["net_total"], scope_aliases={"orders": "orders"}) == (
        '(F.coalesce(F.col("orders.total").cast("decimal(12,2)"), F.lit(0)) - '
        'F.coalesce(F.col("orders.discount").cast("decimal(12,2)"), F.lit(0))).cast(\'decimal(12,2)\')'
    )
    assert PySpark.render.expression()(projection["is_large"], scope_aliases={"orders": "orders"}) == (
        '(F.coalesce(F.col("orders.total").cast("decimal(12,2)"), F.lit(0)) > F.lit(1000))'
    )


def test_v4_expression_renderer_renders_division_modulo_and_negation() -> None:
    class Raw(Schema):
        amount = field.integer(nullable=True)

    class Published(Schema):
        quotient = field.double(nullable=True)
        remainder = field.integer(nullable=True)
        negated = field.integer(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(quotient=row.amount / 2, remainder=row.amount % 2, negated=-row.amount)

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["quotient"], scope_aliases={"rows": "orders"}) == '(F.col("orders.amount") / F.lit(2))'
    assert render(projection["remainder"], scope_aliases={"rows": "orders"}) == '(F.col("orders.amount") % F.lit(2))'
    assert render(projection["negated"], scope_aliases={"rows": "orders"}) == '(-F.col("orders.amount"))'


def test_v4_expression_renderer_renders_typed_bitwise_column_operations() -> None:
    class Raw(Schema):
        flags = field.integer(nullable=False)
        mask = field.long(nullable=False)

    class Published(Schema):
        intersected = field.integer(nullable=False)
        combined = field.long(nullable=False)
        changed = field.long(nullable=False)
        inverted = field.integer(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                intersected=row.flags.bitwise_and(3),
                combined=row.flags.bitwise_or(row.mask),
                changed=row.flags.bitwise_xor(row.mask),
                inverted=row.flags.bitwise_not(),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["intersected"], scope_aliases={"rows": "orders"}) == (
        'F.col("orders.flags").bitwiseAND(F.lit(3))'
    )
    assert render(projection["combined"], scope_aliases={"rows": "orders"}) == (
        'F.col("orders.flags").bitwiseOR(F.col("orders.mask"))'
    )
    assert render(projection["changed"], scope_aliases={"rows": "orders"}) == (
        'F.col("orders.flags").bitwiseXOR(F.col("orders.mask"))'
    )
    assert render(projection["inverted"], scope_aliases={"rows": "orders"}) == 'F.bitwise_not(F.col("orders.flags"))'


def test_v4_expression_renderer_renders_nullif() -> None:
    class Raw(Schema):
        label = field.string(nullable=False)

    class Published(Schema):
        label = field.string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(label=nullif(row.label, "unknown"))

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "orders"}) == (
        'F.nullif(F.col("orders.label"), F.lit(\'unknown\'))'
    )


def test_v4_expression_renderer_renders_remaining_null_control_helpers() -> None:
    class Raw(Schema):
        label = field.string(nullable=True)
        amount = field.decimal(12, 2, nullable=True)

    class Published(Schema):
        nvl_label = field.string(nullable=False)
        ifnull_label = field.string(nullable=False)
        branch_label = field.string(nullable=False)
        amount = field.decimal(12, 2, nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                nvl_label=nvl(row.label, "unknown"),
                ifnull_label=ifnull(row.label, "unknown"),
                branch_label=nvl2(row.label, "known", "unknown"),
                amount=zeroifnull(row.amount),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["nvl_label"], scope_aliases={"rows": "orders"}) == (
        'F.nvl(F.col("orders.label"), F.lit(\'unknown\'))'
    )
    assert render(projection["ifnull_label"], scope_aliases={"rows": "orders"}) == (
        'F.ifnull(F.col("orders.label"), F.lit(\'unknown\'))'
    )
    assert render(projection["branch_label"], scope_aliases={"rows": "orders"}) == (
        'F.nvl2(F.col("orders.label"), F.lit(\'known\'), F.lit(\'unknown\'))'
    )
    assert render(projection["amount"], scope_aliases={"rows": "orders"}) == 'F.zeroifnull(F.col("orders.amount"))'


def test_v4_expression_renderer_renders_nanvl() -> None:
    class Raw(Schema):
        observed = field.double(nullable=True)
        fallback = field.double(nullable=True)

    class Published(Schema):
        observed = field.double(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(observed=nanvl(row.observed, row.fallback))

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "orders"}) == (
        'F.nanvl(F.col("orders.observed"), F.col("orders.fallback"))'
    )


def test_v4_expression_renderer_renders_one_sided_trim() -> None:
    class Raw(Schema):
        label = field.string(nullable=True)

    class Published(Schema):
        left = field.string(nullable=True)
        right = field.string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(left=ltrim(row.label), right=rtrim(row.label))

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["left"], scope_aliases={"rows": "orders"}) == 'F.ltrim(F.col("orders.label"))'
    assert render(projection["right"], scope_aliases={"rows": "orders"}) == 'F.rtrim(F.col("orders.label"))'


def test_v4_expression_renderer_renders_deterministic_numeric_functions() -> None:
    class Raw(Schema):
        amount = field.decimal(12, 2, nullable=True)

    class Published(Schema):
        rounded = field.decimal(12, 1, nullable=True)
        square_root = field.double(nullable=True)
        exponentiated = field.double(nullable=True)
        natural_log = field.double(nullable=True)
        base_ten_log = field.double(nullable=True)
        exponent = field.double(nullable=True)
        sign = field.double(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                rounded=bround(row.amount, scale=1),
                square_root=sqrt(row.amount),
                exponentiated=pow(row.amount, 2),
                natural_log=log(row.amount),
                base_ten_log=log(row.amount, base=10),
                exponent=exp(row.amount),
                sign=signum(row.amount),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["rounded"], scope_aliases={"rows": "orders"}) == 'F.bround(F.col("orders.amount"), 1)'
    assert render(projection["square_root"], scope_aliases={"rows": "orders"}) == 'F.sqrt(F.col("orders.amount"))'
    assert (
        render(projection["exponentiated"], scope_aliases={"rows": "orders"})
        == 'F.pow(F.col("orders.amount"), F.lit(2))'
    )
    assert render(projection["natural_log"], scope_aliases={"rows": "orders"}) == 'F.log(F.col("orders.amount"))'
    assert render(projection["base_ten_log"], scope_aliases={"rows": "orders"}) == 'F.log(10, F.col("orders.amount"))'
    assert render(projection["exponent"], scope_aliases={"rows": "orders"}) == 'F.exp(F.col("orders.amount"))'
    assert render(projection["sign"], scope_aliases={"rows": "orders"}) == 'F.signum(F.col("orders.amount"))'


def test_v4_expression_renderer_renders_temporal_helpers() -> None:
    class Raw(Schema):
        observed_on = field.date(nullable=True)
        observed_at = field.timestamp(nullable=True)
        raw_observed_at = field.string(nullable=False)

    class Published(Schema):
        previous = field.date(nullable=True)
        month_start = field.date(nullable=True)
        year_part = field.integer(nullable=True)
        hour_part = field.integer(nullable=True)
        parsed_date = field.date(nullable=True)
        parsed_timestamp = field.timestamp(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                previous=date_sub(row.observed_on, days=1),
                month_start=trunc(row.observed_on, unit="month"),
                year_part=year(row.observed_on),
                hour_part=hour(row.observed_at),
                parsed_date=to_date(row.raw_observed_at, format="yyyy-MM-dd HH:mm:ss"),
                parsed_timestamp=to_timestamp(row.raw_observed_at, format="yyyy-MM-dd HH:mm:ss"),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert (
        render(projection["previous"], scope_aliases={"rows": "orders"}) == 'F.date_sub(F.col("orders.observed_on"), 1)'
    )
    assert (
        render(projection["month_start"], scope_aliases={"rows": "orders"})
        == 'F.trunc(F.col("orders.observed_on"), \'month\')'
    )
    assert render(projection["year_part"], scope_aliases={"rows": "orders"}) == 'F.year(F.col("orders.observed_on"))'
    assert render(projection["hour_part"], scope_aliases={"rows": "orders"}) == 'F.hour(F.col("orders.observed_at"))'
    assert render(projection["parsed_date"], scope_aliases={"rows": "orders"}) == (
        'F.to_date(F.col("orders.raw_observed_at"), \'yyyy-MM-dd HH:mm:ss\')'
    )
    assert render(projection["parsed_timestamp"], scope_aliases={"rows": "orders"}) == (
        'F.to_timestamp(F.col("orders.raw_observed_at"), \'yyyy-MM-dd HH:mm:ss\')'
    )


def test_v4_expression_renderer_renders_hash_helpers() -> None:
    class Raw(Schema):
        id = field.long(nullable=False)
        label = field.string(nullable=True)

    class Published(Schema):
        hash_code = field.integer(nullable=True)
        long_hash = field.long(nullable=True)
        md5_hash = field.string(nullable=True)
        sha1_hash = field.string(nullable=True)
        sha2_hash = field.string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                hash_code=hash(row.id, row.label),
                long_hash=xxhash64(row.id, row.label),
                md5_hash=md5(row.label),
                sha1_hash=sha1(row.label),
                sha2_hash=sha2(row.label, bits=512),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["hash_code"], scope_aliases={"rows": "orders"}) == (
        'F.hash(F.col("orders.id"), F.col("orders.label"))'
    )
    assert render(projection["long_hash"], scope_aliases={"rows": "orders"}) == (
        'F.xxhash64(F.col("orders.id"), F.col("orders.label"))'
    )
    assert render(projection["md5_hash"], scope_aliases={"rows": "orders"}) == 'F.md5(F.col("orders.label"))'
    assert render(projection["sha1_hash"], scope_aliases={"rows": "orders"}) == 'F.sha1(F.col("orders.label"))'
    assert render(projection["sha2_hash"], scope_aliases={"rows": "orders"}) == 'F.sha2(F.col("orders.label"), 512)'


def test_v1_expression_renderer_renders_join_predicates() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    recipe = PySpark.compiler.lower()(compile_transform(EnrichOrders))
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
    class Raw(Schema):
        promotion_code = field.string(nullable=True, alias='promo-code')

    class Published(Schema):
        promotion_code = field.string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(promotion_code=row.promotion_code)

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "rows"}) == 'F.col("rows.promo-code")'


def test_v1_expression_renderer_renders_nested_struct_construction() -> None:
    class Address(Schema):
        city = field.string(nullable=False)
        postal_code = field.string(nullable=False)

    class Raw(Schema):
        id = field.string(nullable=False)
        shipping = field.struct(Address, nullable=True)

    class Published(Schema):
        id = field.string(nullable=False)
        shipping = field.struct(Address, nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            where(row.shipping.is_not_null())  # type: ignore[attr-defined]
            return Published(
                id=row.id,
                shipping=Address(
                    city=trim(row.shipping.city),  # type: ignore[attr-defined]
                    postal_code=row.shipping.postal_code,  # type: ignore[attr-defined]
                ),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}

    assert PySpark.render.expression()(projection["shipping"], scope_aliases={"rows": "rows"}) == (
        'F.struct(F.trim(F.col("rows.shipping.city")).alias("city"), '
        'F.col("rows.shipping.postal_code").alias("postal_code"))'
    )


def test_v1_expression_renderer_escapes_dotted_nested_field_aliases() -> None:
    class Address(Schema):
        postal_code = field.string(nullable=False, alias='postal.code')

    class Raw(Schema):
        shipping = field.struct(Address, nullable=False)

    class Published(Schema):
        postal_code = field.string(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(postal_code=row.shipping.postal_code)  # type: ignore[attr-defined]

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "rows"}) == (
        'F.col("rows.shipping.`postal.code`")'
    )


def test_v1_expression_renderer_renders_extended_plain_python_expressions() -> None:
    class Raw(Schema):
        customer_id = field.string(nullable=False)
        status = field.string(nullable=True)
        total = field.integer(nullable=False)
        tax = field.integer(nullable=False)
        price = field.integer(nullable=False)
        quantity = field.integer(nullable=False)

    class Published(Schema):
        customer_id = field.string(nullable=False)
        size_tier = field.string(nullable=False)
        is_big = field.boolean(nullable=False)
        is_open = field.boolean(nullable=True)
        is_small = field.boolean(nullable=False)
        is_at_most_sample = field.boolean(nullable=False)
        total_with_tax = field.integer(nullable=False)
        line_total = field.integer(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            order = cast(Any, row)
            return Published(
                customer_id=upper(trim(order.customer_id)),
                size_tier=when(order.total >= 1000, "large").otherwise("standard"),
                is_big=order.total >= 1000,
                is_open=order.status.isin("new", "held"),
                is_small=order.total < 100,
                is_at_most_sample=order.total <= 100,
                total_with_tax=order.total + order.tax,
                line_total=order.price * order.quantity,
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
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
    class Raw(Schema):
        status = field.string(nullable=True)

    class Published(Schema):
        contains_new = field.boolean(nullable=True)
        starts_new = field.boolean(nullable=True)
        ends_new = field.boolean(nullable=True)
        matches_new = field.boolean(nullable=True)
        matches_new_case_insensitive = field.boolean(nullable=True)
        matches_release = field.boolean(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            status = cast(Any, row).status
            return Published(
                contains_new=status.contains("new"),
                starts_new=status.startswith("new"),
                ends_new=status.endswith("new"),
                matches_new=status.like("new%"),
                matches_new_case_insensitive=status.ilike("NEW%"),
                matches_release=status.rlike(r"release-[0-9]+"),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.status").contains(\'new\')',
        'F.col("orders.status").startswith(\'new\')',
        'F.col("orders.status").endswith(\'new\')',
        'F.col("orders.status").like(\'new%\')',
        'F.col("orders.status").ilike(\'NEW%\')',
        "F.col(\"orders.status\").rlike('release-[0-9]+')",
    ]


def test_v3_expression_renderer_renders_collection_indexing() -> None:
    class Raw(Schema):
        tags = field.array(field.string(), contains_null=False, nullable=False)
        attributes = field.map(field.string(), field.string(), value_contains_null=False, nullable=False)

    class Published(Schema):
        first_tag = field.string(nullable=True)
        region = field.string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            source = cast(Any, row)
            return Published(first_tag=source.tags[0], region=source.attributes["region"])

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.tags")[0]',
        'F.col("orders.attributes")[\'region\']',
    ]


def test_v3_expression_renderer_renders_scalar_casts() -> None:
    class Raw(Schema):
        raw_count = field.string(nullable=True)
        count = field.integer(nullable=False)

    class Published(Schema):
        count = field.integer(nullable=True)
        count_text = field.string(nullable=False)
        try_count = field.integer(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            source = cast(Any, row)
            return Published(
                count=source.raw_count.cast(types.integer()),
                count_text=source.count.astype(types.string()),
                try_count=source.raw_count.try_cast(types.integer()),
            )

    plan = compile_transform(Publish)
    with pytest.raises(BackendCapabilityError):
        PySpark.compiler.lower()(plan)

    recipe = PySpark.compiler.lower()(plan, capabilities=PySparkCapabilities(target_profile=">=4.0,<4.1"))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.raw_count").cast(\'int\')',
        'F.col("orders.count").cast(\'string\')',
        'F.col("orders.raw_count").try_cast(\'int\')',
    ]


def test_v3_expression_renderer_renders_string_sql_helpers() -> None:
    class Raw(Schema):
        label = field.string(nullable=True)

    class Published(Schema):
        prefix = field.string(nullable=True)
        parts = field.array(field.string(), contains_null=False, nullable=True)
        normalized = field.string(nullable=True)
        extracted = field.string(nullable=True)
        character_count = field.integer(nullable=True)
        title = field.string(nullable=True)
        backward = field.string(nullable=True)
        normalized_letters = field.string(nullable=True)
        dash_position = field.integer(nullable=True)
        distance = field.integer(nullable=True)
        joined = field.string(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                prefix=substring(row.label, start=1, length=3),
                parts=split(row.label, pattern="-"),
                normalized=regexp_replace(row.label, pattern=r"\s+", replacement=" "),
                extracted=regexp_extract(row.label, pattern=r"^([^-]+)", group=1),
                character_count=length(row.label),
                title=initcap(row.label),
                backward=reverse(row.label),
                normalized_letters=translate(row.label, matching="-", replacement="_"),
                dash_position=instr(row.label, substring="-"),
                distance=levenshtein(row.label, "release"),
                joined=concat_ws(" / ", row.label, "release"),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
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
    class Raw(Schema):
        start_date = field.date(nullable=False)
        end_date = field.date(nullable=True)
        recorded_at = field.timestamp(nullable=True)

    class Published(Schema):
        due_date = field.date(nullable=False)
        elapsed_days = field.integer(nullable=True)
        month = field.timestamp(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                due_date=date_add(row.start_date, days=7),
                elapsed_days=datediff(row.end_date, row.start_date),
                month=date_trunc(row.recorded_at, unit="month"),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.date_add(F.col("orders.start_date"), 7)',
        'F.datediff(F.col("orders.end_date"), F.col("orders.start_date"))',
        'F.date_trunc(\'month\', F.col("orders.recorded_at"))',
    ]


def test_v3_expression_renderer_renders_numeric_sql_helpers() -> None:
    class Raw(Schema):
        amount = field.decimal(precision=12, scale=2, nullable=True)

    class Published(Schema):
        absolute_amount = field.decimal(precision=12, scale=2, nullable=True)
        rounded_amount = field.decimal(precision=12, scale=1, nullable=True)
        ceiling = field.decimal(precision=11, scale=0, nullable=True)
        floor = field.decimal(precision=11, scale=0, nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                absolute_amount=abs(row.amount),
                rounded_amount=round(row.amount, scale=1),
                ceiling=ceil(row.amount),
                floor=floor(row.amount),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.abs(F.col("orders.amount"))',
        'F.round(F.col("orders.amount"), 1)',
        'F.ceil(F.col("orders.amount"))',
        'F.floor(F.col("orders.amount"))',
    ]


def test_v3_expression_renderer_renders_predicate_sql_helpers() -> None:
    class Raw(Schema):
        label = field.string(nullable=True)
        score = field.double(nullable=True)

    class Published(Schema):
        missing_label = field.boolean(nullable=False)
        present_label = field.boolean(nullable=False)
        invalid_score = field.boolean(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                missing_label=isnull(row.label),
                present_label=isnotnull(row.label),
                invalid_score=isnan(row.score),
            )

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.label").isNull()',
        'F.col("orders.label").isNotNull()',
        'F.isnan(F.col("orders.score"))',
    ]


def test_v3_expression_renderer_renders_struct_get_field() -> None:
    class Address(Schema):
        city = field.string(nullable=False, alias='city-name')

    class Raw(Schema):
        address = field.struct(Address, nullable=True)

    class Published(Schema):
        city = field.string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(city=cast(Any, row).address.get_field("city"))

    recipe = PySpark.compiler.lower()(compile_transform(Publish))
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "orders"}) == (
        'F.col("orders.address").getField(\'city-name\')'
    )
