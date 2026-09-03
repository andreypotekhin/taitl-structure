from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.core.target.capabilities.api import BackendCapabilityError
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.expressions import replace as replace_text


def _recipe(transform) -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(transform, materialize_schemas=False).lowered)


def test_v1_expression_renderer_renders_filter_helpers_and_literals() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    recipe = _recipe(EnrichOrders)
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
    from testing.model.orders.transforms.order import EnrichOrders

    recipe = _recipe(EnrichOrders)
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
        amount = integer(nullable=True)

    class Published(Schema):
        quotient = double(nullable=True)
        remainder = integer(nullable=True)
        negated = integer(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(quotient=row.amount / 2, remainder=row.amount % 2, negated=-row.amount)

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["quotient"], scope_aliases={"rows": "orders"}) == '(F.col("orders.amount") / F.lit(2))'
    assert render(projection["remainder"], scope_aliases={"rows": "orders"}) == '(F.col("orders.amount") % F.lit(2))'
    assert render(projection["negated"], scope_aliases={"rows": "orders"}) == '(-F.col("orders.amount"))'


def test_v4_expression_renderer_renders_typed_bitwise_column_operations() -> None:
    class Raw(Schema):
        flags = integer(nullable=False)
        mask = long(nullable=False)

    class Published(Schema):
        intersected = integer(nullable=False)
        combined = long(nullable=False)
        changed = long(nullable=False)
        inverted = integer(nullable=False)

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

    recipe = _recipe(Publish)
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
        label = string(nullable=False)

    class Published(Schema):
        label = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(label=nullif(row.label, "unknown"))

    recipe = _recipe(Publish)
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "orders"}) == (
        'F.nullif(F.col("orders.label"), F.lit(\'unknown\'))'
    )


def test_v4_expression_renderer_renders_remaining_null_control_helpers() -> None:
    class Raw(Schema):
        label = string(nullable=True)
        amount = decimal(12, 2, nullable=True)

    class Published(Schema):
        nvl_label = string(nullable=False)
        ifnull_label = string(nullable=False)
        branch_label = string(nullable=False)
        amount = decimal(12, 2, nullable=False)

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

    recipe = _recipe(Publish)
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


def test_v7_expression_renderer_renders_binary_encoding_helpers() -> None:
    class Raw(Schema):
        payload = binary(nullable=True)
        text = string(nullable=True)

    class Published(Schema):
        base64_text = string(nullable=True)
        decoded_payload = binary(nullable=True)
        encoded_text = binary(nullable=True)
        decoded_text = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                base64_text=base64(row.payload),
                decoded_payload=unbase64(row.text),
                encoded_text=encode(row.text, charset="UTF-8"),
                decoded_text=decode(row.payload, charset="UTF-8"),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["base64_text"], scope_aliases={"rows": "raw"}) == 'F.base64(F.col("raw.payload"))'
    assert render(projection["decoded_payload"], scope_aliases={"rows": "raw"}) == 'F.unbase64(F.col("raw.text"))'
    assert render(projection["encoded_text"], scope_aliases={"rows": "raw"}) == (
        'F.encode(F.col("raw.text"), \'UTF-8\')'
    )
    assert render(projection["decoded_text"], scope_aliases={"rows": "raw"}) == (
        'F.decode(F.col("raw.payload"), \'UTF-8\')'
    )


def test_v7_expression_renderer_renders_schema_carrying_parsing_helpers() -> None:
    class Details(Schema):
        region = string(nullable=True)

    class Payload(Schema):
        code = string(nullable=True)
        amount = integer(nullable=True)
        details = struct(Details, nullable=True)

    class Raw(Schema):
        payload_json = string(nullable=True)
        payload_csv = string(nullable=True)
        payload = struct(Payload, nullable=True)

    class Published(Schema):
        from_json_payload = struct(Payload, nullable=True)
        from_csv_payload = struct(Payload, nullable=True)
        payload_json = string(nullable=True)
        payload_csv = string(nullable=True)
        json_value = string(nullable=True)
        json_length = integer(nullable=True)
        json_keys = array(string(), contains_null=False, nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                from_json_payload=from_json(row.payload_json, as_=Payload),
                from_csv_payload=from_csv(row.payload_csv, as_=Payload, options=CsvOptions(delimiter="|")),
                payload_json=to_json(row.payload),
                payload_csv=to_csv(row.payload, options=CsvOptions(delimiter="|")),
                json_value=get_json_object(row.payload_json, "$.customer.id"),
                json_length=json_array_length(row.payload_json),
                json_keys=json_object_keys(row.payload_json),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    inline_schema = (
        'T.StructType([T.StructField("code", T.StringType(), True), '
        'T.StructField("amount", T.IntegerType(), True), '
        'T.StructField("details", T.StructType([T.StructField("region", T.StringType(), True)]), True)])'
    )
    assert render(projection["from_json_payload"], scope_aliases={"rows": "raw"}) == (
        f'F.from_json(F.col("raw.payload_json"), {inline_schema}, {{\'mode\': \'PERMISSIVE\'}})'
    )
    assert render(projection["from_csv_payload"], scope_aliases={"rows": "raw"}) == (
        'F.from_csv(F.col("raw.payload_csv"), '
        "'code STRING, amount INT, details STRUCT<region:STRING>', {'sep': '|', 'mode': 'PERMISSIVE'})"
    )
    assert render(projection["payload_json"], scope_aliases={"rows": "raw"}) == 'F.to_json(F.col("raw.payload"))'
    assert render(projection["payload_csv"], scope_aliases={"rows": "raw"}) == (
        'F.to_csv(F.col("raw.payload"), {\'sep\': \'|\'})'
    )
    assert render(projection["json_value"], scope_aliases={"rows": "raw"}) == (
        'F.get_json_object(F.col("raw.payload_json"), \'$.customer.id\')'
    )
    assert render(projection["json_length"], scope_aliases={"rows": "raw"}) == (
        'F.json_array_length(F.col("raw.payload_json"))'
    )
    assert render(projection["json_keys"], scope_aliases={"rows": "raw"}) == (
        'F.json_object_keys(F.col("raw.payload_json"))'
    )


def test_v4_expression_renderer_renders_nanvl() -> None:
    class Raw(Schema):
        observed = double(nullable=True)
        fallback = double(nullable=True)

    class Published(Schema):
        observed = double(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(observed=nanvl(row.observed, row.fallback))

    recipe = _recipe(Publish)
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "orders"}) == (
        'F.nanvl(F.col("orders.observed"), F.col("orders.fallback"))'
    )


def test_v4_expression_renderer_renders_one_sided_trim() -> None:
    class Raw(Schema):
        label = string(nullable=True)

    class Published(Schema):
        left = string(nullable=True)
        right = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(left=ltrim(row.label), right=rtrim(row.label))

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["left"], scope_aliases={"rows": "orders"}) == 'F.ltrim(F.col("orders.label"))'
    assert render(projection["right"], scope_aliases={"rows": "orders"}) == 'F.rtrim(F.col("orders.label"))'


def test_v4_expression_renderer_renders_deterministic_numeric_functions() -> None:
    class Raw(Schema):
        amount = decimal(12, 2, nullable=True)

    class Published(Schema):
        rounded = decimal(12, 1, nullable=True)
        square_root = double(nullable=True)
        exponentiated = double(nullable=True)
        natural_log = double(nullable=True)
        base_ten_log = double(nullable=True)
        exponent = double(nullable=True)
        sign = double(nullable=True)
        arc_cosine = double(nullable=True)
        hypotenuse = double(nullable=True)

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
                arc_cosine=acos(row.amount),
                hypotenuse=hypot(row.amount, 2),
            )

    recipe = _recipe(Publish)
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
    assert render(projection["arc_cosine"], scope_aliases={"rows": "orders"}) == 'F.acos(F.col("orders.amount"))'
    assert render(projection["hypotenuse"], scope_aliases={"rows": "orders"}) == (
        'F.hypot(F.col("orders.amount"), F.lit(2))'
    )


def test_v4_expression_renderer_renders_trigonometric_and_logarithmic_functions() -> None:
    class Raw(Schema):
        angle = double(nullable=True)
        x = double(nullable=False)
        y = double(nullable=True)

    class Published(Schema):
        arc_sine = double(nullable=True)
        arc_tangent = double(nullable=True)
        arc_tangent_two = double(nullable=True)
        cosine = double(nullable=True)
        degrees_value = double(nullable=True)
        natural_log = double(nullable=True)
        base_ten_log = double(nullable=True)
        radians_value = double(nullable=True)
        sine = double(nullable=True)
        tangent = double(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                arc_sine=asin(row.angle),
                arc_tangent=atan(row.angle),
                arc_tangent_two=atan2(row.y, row.x),
                cosine=cos(row.angle),
                degrees_value=degrees(row.angle),
                natural_log=ln(row.angle),
                base_ten_log=log10(row.angle),
                radians_value=radians(row.angle),
                sine=sin(row.angle),
                tangent=tan(row.angle),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["arc_sine"], scope_aliases={"rows": "orders"}) == 'F.asin(F.col("orders.angle"))'
    assert render(projection["arc_tangent"], scope_aliases={"rows": "orders"}) == 'F.atan(F.col("orders.angle"))'
    assert render(projection["arc_tangent_two"], scope_aliases={"rows": "orders"}) == (
        'F.atan2(F.col("orders.y"), F.col("orders.x"))'
    )
    assert render(projection["cosine"], scope_aliases={"rows": "orders"}) == 'F.cos(F.col("orders.angle"))'
    assert render(projection["degrees_value"], scope_aliases={"rows": "orders"}) == (
        'F.degrees(F.col("orders.angle"))'
    )
    assert render(projection["natural_log"], scope_aliases={"rows": "orders"}) == 'F.ln(F.col("orders.angle"))'
    assert render(projection["base_ten_log"], scope_aliases={"rows": "orders"}) == (
        'F.log10(F.col("orders.angle"))'
    )
    assert render(projection["radians_value"], scope_aliases={"rows": "orders"}) == (
        'F.radians(F.col("orders.angle"))'
    )
    assert render(projection["sine"], scope_aliases={"rows": "orders"}) == 'F.sin(F.col("orders.angle"))'
    assert render(projection["tangent"], scope_aliases={"rows": "orders"}) == 'F.tan(F.col("orders.angle"))'


def test_v4_expression_renderer_renders_hyperbolic_and_extended_numeric_functions() -> None:
    class Raw(Schema):
        amount = double(nullable=True)

    class Published(Schema):
        acosh_value = double(nullable=True)
        asinh_value = double(nullable=True)
        atanh_value = double(nullable=True)
        cbrt_value = double(nullable=True)
        cosh_value = double(nullable=True)
        cot_value = double(nullable=True)
        csc_value = double(nullable=True)
        expm1_value = double(nullable=True)
        log1p_value = double(nullable=True)
        log2_value = double(nullable=True)
        rint_value = double(nullable=True)
        sec_value = double(nullable=True)
        sign_value = double(nullable=True)
        sinh_value = double(nullable=True)
        tanh_value = double(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                acosh_value=acosh(row.amount),
                asinh_value=asinh(row.amount),
                atanh_value=atanh(row.amount),
                cbrt_value=cbrt(row.amount),
                cosh_value=cosh(row.amount),
                cot_value=cot(row.amount),
                csc_value=csc(row.amount),
                expm1_value=expm1(row.amount),
                log1p_value=log1p(row.amount),
                log2_value=log2(row.amount),
                rint_value=rint(row.amount),
                sec_value=sec(row.amount),
                sign_value=sign(row.amount),
                sinh_value=sinh(row.amount),
                tanh_value=tanh(row.amount),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    expected = {
        "acosh_value": "acosh",
        "asinh_value": "asinh",
        "atanh_value": "atanh",
        "cbrt_value": "cbrt",
        "cosh_value": "cosh",
        "cot_value": "cot",
        "csc_value": "csc",
        "expm1_value": "expm1",
        "log1p_value": "log1p",
        "log2_value": "log2",
        "rint_value": "rint",
        "sec_value": "sec",
        "sign_value": "sign",
        "sinh_value": "sinh",
        "tanh_value": "tanh",
    }
    for field, function in expected.items():
        assert render(projection[field], scope_aliases={"rows": "orders"}) == (
            f'F.{function}(F.col("orders.amount"))'
        )


def test_v4_expression_renderer_renders_remaining_admitted_numeric_functions() -> None:
    class Raw(Schema):
        amount = integer(nullable=True)
        digits = string(nullable=True)

    class Published(Schema):
        e_value = double(nullable=False)
        pi_value = double(nullable=False)
        binary_value = string(nullable=True)
        hexadecimal_value = string(nullable=True)
        decoded_value = binary(nullable=True)
        factorial_value = long(nullable=True)
        greatest_value = integer(nullable=True)
        least_value = integer(nullable=True)
        pmod_value = integer(nullable=True)
        converted_value = string(nullable=True)
        bucket_value = integer(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                e_value=e(),
                pi_value=pi(),
                binary_value=bin(row.amount),
                hexadecimal_value=hex(row.amount),
                decoded_value=unhex("ff"),
                factorial_value=factorial(row.amount),
                greatest_value=greatest(row.amount, 2),
                least_value=least(row.amount, 2),
                pmod_value=pmod(row.amount, 2),
                converted_value=conv(row.digits, from_base=2, to_base=16),
                bucket_value=width_bucket(row.amount, 0, 100, num_buckets=10),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    expected = {
        "e_value": "F.e()",
        "pi_value": "F.pi()",
        "binary_value": 'F.bin(F.col("orders.amount"))',
        "hexadecimal_value": 'F.hex(F.col("orders.amount"))',
        "decoded_value": "F.unhex(F.lit('ff'))",
        "factorial_value": 'F.factorial(F.col("orders.amount"))',
        "greatest_value": 'F.greatest(F.col("orders.amount"), F.lit(2))',
        "least_value": 'F.least(F.col("orders.amount"), F.lit(2))',
        "pmod_value": 'F.pmod(F.col("orders.amount"), F.lit(2))',
        "converted_value": 'F.conv(F.col("orders.digits"), 2, 16)',
        "bucket_value": 'F.width_bucket(F.col("orders.amount"), F.lit(0), F.lit(100), 10)',
    }
    for field, expected_render in expected.items():
        assert render(projection[field], scope_aliases={"rows": "orders"}) == expected_render


def test_v4_expression_renderer_renders_seeded_and_unseeded_rand() -> None:
    class Raw(Schema):
        amount = double(nullable=False)

    class Published(Schema):
        seeded = double(nullable=False)
        unseeded = double(nullable=False)
        normal = double(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(seeded=rand(seed=17), unseeded=rand(reproducible=False), normal=randn(seed=17))

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["seeded"], scope_aliases={"rows": "orders"}) == "F.rand(seed=17)"
    assert render(projection["unseeded"], scope_aliases={"rows": "orders"}) == "F.rand()"
    assert render(projection["normal"], scope_aliases={"rows": "orders"}) == "F.randn(seed=17)"


def test_v4_expression_renderer_renders_temporal_helpers() -> None:
    class Raw(Schema):
        observed_on = date(nullable=True)
        observed_at = timestamp(nullable=True)
        raw_observed_at = string(nullable=False)

    class Published(Schema):
        previous = date(nullable=True)
        month_start = date(nullable=True)
        year_part = integer(nullable=True)
        hour_part = integer(nullable=True)
        parsed_date = date(nullable=True)
        parsed_timestamp = timestamp(nullable=True)

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

    recipe = _recipe(Publish)
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


def test_v4_expression_renderer_renders_calendar_and_padding_helpers() -> None:
    class Raw(Schema):
        observed_on = date(nullable=True)
        label = string(nullable=True)

    class Published(Schema):
        shifted = date(nullable=True)
        following_monday = date(nullable=True)
        left_padded = string(nullable=True)
        right_padded = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                shifted=add_months(row.observed_on, months=2),
                following_monday=next_day(row.observed_on, day_of_week="Mon"),
                left_padded=lpad(row.label, length=8, pad="0"),
                right_padded=rpad(row.label, length=8, pad="0"),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["shifted"], scope_aliases={"rows": "orders"}) == (
        'F.add_months(F.col("orders.observed_on"), 2)'
    )
    assert render(projection["following_monday"], scope_aliases={"rows": "orders"}) == (
        'F.next_day(F.col("orders.observed_on"), \'Mon\')'
    )
    assert render(projection["left_padded"], scope_aliases={"rows": "orders"}) == (
        'F.lpad(F.col("orders.label"), 8, \'0\')'
    )
    assert render(projection["right_padded"], scope_aliases={"rows": "orders"}) == (
        'F.rpad(F.col("orders.label"), 8, \'0\')'
    )


def test_v4_expression_renderer_renders_string_position_and_slicing_helpers() -> None:
    class Raw(Schema):
        label = string(nullable=True)

    class Published(Schema):
        ascii_code = integer(nullable=True)
        character_count = integer(nullable=True)
        first_three = string(nullable=True)
        last_three = string(nullable=True)
        located = integer(nullable=True)
        byte_count = integer(nullable=True)
        repeated = string(nullable=True)
        replaced = string(nullable=True)
        path_prefix = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                ascii_code=ascii(row.label),
                character_count=char_length(row.label),
                first_three=left(row.label, length=3),
                last_three=right(row.label, length=3),
                located=locate(row.label, substring="Ada", position=2),
                byte_count=octet_length(row.label),
                repeated=repeat(row.label, count=2),
                replaced=replace_text(row.label, search="-", replacement="_"),
                path_prefix=substring_index(row.label, delimiter="/", count=2),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["ascii_code"], scope_aliases={"rows": "orders"}) == 'F.ascii(F.col("orders.label"))'
    assert render(projection["character_count"], scope_aliases={"rows": "orders"}) == (
        'F.char_length(F.col("orders.label"))'
    )
    assert render(projection["first_three"], scope_aliases={"rows": "orders"}) == (
        'F.left(F.col("orders.label"), 3)'
    )
    assert render(projection["last_three"], scope_aliases={"rows": "orders"}) == (
        'F.right(F.col("orders.label"), 3)'
    )
    assert render(projection["located"], scope_aliases={"rows": "orders"}) == (
        "F.locate('Ada', F.col(\"orders.label\"), 2)"
    )
    assert render(projection["byte_count"], scope_aliases={"rows": "orders"}) == (
        'F.octet_length(F.col("orders.label"))'
    )
    assert render(projection["repeated"], scope_aliases={"rows": "orders"}) == (
        'F.repeat(F.col("orders.label"), 2)'
    )
    assert render(projection["replaced"], scope_aliases={"rows": "orders"}) == (
        "F.replace(F.col(\"orders.label\"), '-', '_')"
    )
    assert render(projection["path_prefix"], scope_aliases={"rows": "orders"}) == (
        "F.substring_index(F.col(\"orders.label\"), '/', 2)"
    )


def test_v4_expression_renderer_renders_hash_helpers() -> None:
    class Raw(Schema):
        id = long(nullable=False)
        label = string(nullable=True)

    class Published(Schema):
        hash_code = integer(nullable=True)
        long_hash = long(nullable=True)
        crc32_hash = long(nullable=True)
        md5_hash = string(nullable=True)
        sha1_hash = string(nullable=True)
        sha2_hash = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                hash_code=hash(row.id, row.label),
                long_hash=xxhash64(row.id, row.label),
                crc32_hash=crc32(row.label),
                md5_hash=md5(row.label),
                sha1_hash=sha1(row.label),
                sha2_hash=sha2(row.label, bits=512),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["hash_code"], scope_aliases={"rows": "orders"}) == (
        'F.hash(F.col("orders.id"), F.col("orders.label"))'
    )
    assert render(projection["long_hash"], scope_aliases={"rows": "orders"}) == (
        'F.xxhash64(F.col("orders.id"), F.col("orders.label"))'
    )
    assert render(projection["crc32_hash"], scope_aliases={"rows": "orders"}) == 'F.crc32(F.col("orders.label"))'
    assert render(projection["md5_hash"], scope_aliases={"rows": "orders"}) == 'F.md5(F.col("orders.label"))'
    assert render(projection["sha1_hash"], scope_aliases={"rows": "orders"}) == 'F.sha1(F.col("orders.label"))'
    assert render(projection["sha2_hash"], scope_aliases={"rows": "orders"}) == 'F.sha2(F.col("orders.label"), 512)'


def test_v1_expression_renderer_renders_join_predicates() -> None:
    from testing.model.orders.transforms.order import EnrichOrders

    recipe = _recipe(EnrichOrders)
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
        promotion_code = string(nullable=True, alias='promo-code')

    class Published(Schema):
        promotion_code = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(promotion_code=row.promotion_code)

    recipe = _recipe(Publish)
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "rows"}) == 'F.col("rows.promo-code")'


def test_v1_expression_renderer_renders_nested_struct_construction() -> None:
    class Address(Schema):
        city = string(nullable=False)
        postal_code = string(nullable=False)

    class Raw(Schema):
        id = string(nullable=False)
        shipping = struct(Address, nullable=True)

    class Published(Schema):
        id = string(nullable=False)
        shipping = struct(Address, nullable=False)

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

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}

    assert PySpark.render.expression()(projection["shipping"], scope_aliases={"rows": "rows"}) == (
        'F.struct(F.trim(F.col("rows.shipping.city")).alias("city"), '
        'F.col("rows.shipping.postal_code").alias("postal_code"))'
    )


def test_v1_expression_renderer_escapes_dotted_nested_field_aliases() -> None:
    class Address(Schema):
        postal_code = string(nullable=False, alias='postal.code')

    class Raw(Schema):
        shipping = struct(Address, nullable=False)

    class Published(Schema):
        postal_code = string(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(postal_code=row.shipping.postal_code)  # type: ignore[attr-defined]

    recipe = _recipe(Publish)
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "rows"}) == (
        'F.col("rows.shipping.`postal.code`")'
    )


def test_v1_expression_renderer_renders_extended_plain_python_expressions() -> None:
    class Raw(Schema):
        customer_id = string(nullable=False)
        status = string(nullable=True)
        total = integer(nullable=False)
        tax = integer(nullable=False)
        price = integer(nullable=False)
        quantity = integer(nullable=False)

    class Published(Schema):
        customer_id = string(nullable=False)
        size_tier = string(nullable=False)
        is_big = boolean(nullable=False)
        is_open = boolean(nullable=True)
        is_small = boolean(nullable=False)
        is_at_most_sample = boolean(nullable=False)
        total_with_tax = integer(nullable=False)
        line_total = integer(nullable=False)

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

    recipe = _recipe(Publish)
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
        status = string(nullable=True)

    class Published(Schema):
        contains_new = boolean(nullable=True)
        starts_new = boolean(nullable=True)
        ends_new = boolean(nullable=True)
        matches_new = boolean(nullable=True)
        matches_new_case_insensitive = boolean(nullable=True)
        matches_release = boolean(nullable=True)

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

    recipe = _recipe(Publish)
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
        tags = array(string(), contains_null=False, nullable=False)
        attributes = map(string(), string(), value_contains_null=False, nullable=False)

    class Published(Schema):
        first_tag = string(nullable=True)
        region = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            source = cast(Any, row)
            return Published(first_tag=source.tags[0], region=source.attributes["region"])

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.tags")[0]',
        'F.col("orders.attributes")[\'region\']',
    ]


def test_v3_expression_renderer_renders_scalar_casts() -> None:
    class Raw(Schema):
        raw_count = string(nullable=True)
        count = integer(nullable=False)

    class Published(Schema):
        count = integer(nullable=True)
        count_text = string(nullable=False)
        try_count = integer(nullable=True)

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

    with pytest.raises(BackendCapabilityError):
        Compiler.frontend.compile()(Publish, materialize_schemas=False)

    recipe = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(
            Publish,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.0,<4.1"}},
        ).lowered,
    )
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.raw_count").cast(\'int\')',
        'F.col("orders.count").cast(\'string\')',
        'F.col("orders.raw_count").try_cast(\'int\')',
    ]


def test_v3_expression_renderer_renders_string_sql_helpers() -> None:
    class Raw(Schema):
        label = string(nullable=True)
        parts = array(string(), contains_null=False, nullable=False)

    class Published(Schema):
        prefix = string(nullable=True)
        parts = array(string(), contains_null=False, nullable=True)
        normalized = string(nullable=True)
        extracted = string(nullable=True)
        character_count = integer(nullable=True)
        title = string(nullable=True)
        backward = string(nullable=True)
        normalized_letters = string(nullable=True)
        dash_position = integer(nullable=True)
        distance = integer(nullable=True)
        joined = string(nullable=False)
        path = string(nullable=False)

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
                path=concat_ws("\u001f", row.parts),
            )

    recipe = _recipe(Publish)
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
        'F.concat_ws(\'\\x1f\', F.col("orders.parts"))',
    ]


def test_v4_expression_renderer_renders_elt_and_substr() -> None:
    class Raw(Schema):
        label = string(nullable=True)

    class Published(Schema):
        selected = string(nullable=True)
        shortened = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                selected=elt(2, "first", row.label),
                shortened=substr(row.label, start=1, length=4),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.elt(F.lit(2), F.lit(\'first\'), F.col("orders.label"))',
        'F.substr(F.col("orders.label"), 1, 4)',
    ]


def test_v4_expression_renderer_renders_format_string_and_printf() -> None:
    class Raw(Schema):
        label = string(nullable=True)

    class Published(Schema):
        formatted = string(nullable=True)
        printed = string(nullable=False)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                formatted=format_string("label=%s", row.label),
                printed=printf("status=%s", "ready"),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.format_string(\'label=%s\', F.col("orders.label"))',
        "F.printf('status=%s', F.lit('ready'))",
    ]


def test_v3_expression_renderer_renders_temporal_sql_helpers() -> None:
    class Raw(Schema):
        start_date = date(nullable=False)
        end_date = date(nullable=True)
        recorded_at = timestamp(nullable=True)

    class Published(Schema):
        due_date = date(nullable=False)
        elapsed_days = integer(nullable=True)
        month = timestamp(nullable=True)

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

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.date_add(F.col("orders.start_date"), 7)',
        'F.datediff(F.col("orders.end_date"), F.col("orders.start_date"))',
        'F.date_trunc(\'month\', F.col("orders.recorded_at"))',
    ]


def test_v3_expression_renderer_renders_numeric_sql_helpers() -> None:
    class Raw(Schema):
        amount = decimal(precision=12, scale=2, nullable=True)

    class Published(Schema):
        absolute_amount = decimal(precision=12, scale=2, nullable=True)
        rounded_amount = decimal(precision=12, scale=1, nullable=True)
        ceiling = decimal(precision=11, scale=0, nullable=True)
        floor = decimal(precision=11, scale=0, nullable=True)

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

    recipe = _recipe(Publish)
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
        label = string(nullable=True)
        score = double(nullable=True)

    class Published(Schema):
        missing_label = boolean(nullable=False)
        present_label = boolean(nullable=False)
        invalid_score = boolean(nullable=False)

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

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert [render(expression, scope_aliases={"rows": "orders"}) for expression in projection.values()] == [
        'F.col("orders.label").isNull()',
        'F.col("orders.label").isNotNull()',
        'F.isnan(F.col("orders.score"))',
    ]


def test_v3_expression_renderer_renders_struct_get_field() -> None:
    class Address(Schema):
        city = string(nullable=False, alias='city-name')

    class Raw(Schema):
        address = struct(Address, nullable=True)

    class Published(Schema):
        city = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(city=cast(Any, row).address.get_field("city"))

    recipe = _recipe(Publish)
    expression = recipe.steps[0].projection[0].expression

    assert PySpark.render.expression()(expression, scope_aliases={"rows": "orders"}) == (
        'F.col("orders.address").getField(\'city-name\')'
    )


def test_v4_expression_renderer_renders_extended_string_helpers() -> None:
    class Raw(Schema):
        label = string(nullable=True)
        candidates = string(nullable=False)
        amount = decimal(12, 2, nullable=True)

    class Published(Schema):
        cleaned = string(nullable=True)
        present = boolean(nullable=True)
        index = integer(nullable=True)
        formatted = string(nullable=True)
        located = integer(nullable=True)
        part = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                cleaned=btrim(row.label, trim="0"),
                present=contains(row.label, "Ada"),
                index=find_in_set(row.label, row.candidates),
                formatted=format_number(row.amount, decimals=2),
                located=position("Ada", row.label, start=2),
                part=split_part(row.candidates, ",", 2),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["cleaned"], scope_aliases={"rows": "orders"}) == (
        'F.btrim(F.col("orders.label"), \'0\')'
    )
    assert render(projection["present"], scope_aliases={"rows": "orders"}) == (
        'F.contains(F.col("orders.label"), F.lit(\'Ada\'))'
    )
    assert render(projection["index"], scope_aliases={"rows": "orders"}) == (
        'F.find_in_set(F.col("orders.label"), F.col("orders.candidates"))'
    )
    assert render(projection["formatted"], scope_aliases={"rows": "orders"}) == (
        'F.format_number(F.col("orders.amount"), 2)'
    )
    assert render(projection["located"], scope_aliases={"rows": "orders"}) == (
        'F.position(F.lit(\'Ada\'), F.col("orders.label"), 2)'
    )
    assert render(projection["part"], scope_aliases={"rows": "orders"}) == (
        'F.split_part(F.col("orders.candidates"), F.lit(\',\'), F.lit(2))'
    )


def test_v4_expression_renderer_renders_character_and_regex_helpers() -> None:
    class Raw(Schema):
        label = string(nullable=True)
        code_point = integer(nullable=False)

    class Published(Schema):
        character = string(nullable=False)
        sounds_like = string(nullable=True)
        count = integer(nullable=True)
        matches = array(string(), contains_null=False, nullable=True)
        match_position = integer(nullable=True)
        match_text = string(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                character=char(row.code_point),
                sounds_like=soundex(row.label),
                count=regexp_count(row.label, pattern="Ada"),
                matches=regexp_extract_all(row.label, pattern="(Ada)", group=1),
                match_position=regexp_instr(row.label, pattern="Ada"),
                match_text=regexp_substr(row.label, pattern="Ada"),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["character"], scope_aliases={"rows": "orders"}) == (
        'F.char(F.col("orders.code_point"))'
    )
    assert render(projection["sounds_like"], scope_aliases={"rows": "orders"}) == (
        'F.soundex(F.col("orders.label"))'
    )
    assert render(projection["count"], scope_aliases={"rows": "orders"}) == (
        'F.regexp_count(F.col("orders.label"), \'Ada\')'
    )
    assert render(projection["matches"], scope_aliases={"rows": "orders"}) == (
        'F.regexp_extract_all(F.col("orders.label"), \'(Ada)\', 1)'
    )
    assert render(projection["match_position"], scope_aliases={"rows": "orders"}) == (
        'F.regexp_instr(F.col("orders.label"), \'Ada\', 0)'
    )
    assert render(projection["match_text"], scope_aliases={"rows": "orders"}) == (
        'F.regexp_substr(F.col("orders.label"), \'Ada\')'
    )


def test_v4_expression_renderer_renders_sql_bitwise_helpers() -> None:
    class Raw(Schema):
        flags = long(nullable=True)
        position = integer(nullable=False)

    class Published(Schema):
        count = long(nullable=True)
        selected = integer(nullable=True)
        selected_alias = integer(nullable=True)

    @transform
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(
                count=bit_count(row.flags),
                selected=bit_get(row.flags, row.position),
                selected_alias=getbit(row.flags, row.position),
            )

    recipe = _recipe(Publish)
    projection = {assignment.field.name: assignment.expression for assignment in recipe.steps[0].projection}
    render = PySpark.render.expression()

    assert render(projection["count"], scope_aliases={"rows": "orders"}) == (
        'F.bit_count(F.col("orders.flags"))'
    )
    assert render(projection["selected"], scope_aliases={"rows": "orders"}) == (
        'F.bit_get(F.col("orders.flags"), F.col("orders.position"))'
    )
    assert render(projection["selected_alias"], scope_aliases={"rows": "orders"}) == (
        'F.getbit(F.col("orders.flags"), F.col("orders.position"))'
    )
