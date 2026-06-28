def test_class_local_expression_helpers_lower_to_symbolic_calls(orders_plan) -> None:
    """I can define class-local expression helpers."""

    projection = {assignment.field.name: assignment.expression for assignment in orders_plan.steps[0].projection}
    customer_id = projection["customer_id"]

    assert customer_id.kind == "call"
    assert customer_id.data == {"function": "lower"}
    assert customer_id.args[0].kind == "call"
    assert customer_id.args[0].data == {"function": "trim"}
    field = customer_id.args[0].args[0].data
    assert {key: field[key] for key in ("scope", "field")} == {"scope": "orders", "field": "customer_id"}


def test_expression_helpers_can_be_called_through_self(orders_transform_text) -> None:
    """I can call class-local expression helpers through self."""

    assert 'F.lower(F.trim(F.col("order_raw.id"))).cast(T.StringType()).alias("id")' in orders_transform_text
    assert (
        'F.lower(F.trim(F.col("order_raw.customer_id"))).cast(T.StringType()).alias("customer_id")'
        in orders_transform_text
    )
    assert (
        'F.lower(F.trim(F.col("order_raw.product_id"))).cast(T.StringType()).alias("product_id")'
        in orders_transform_text
    )


def test_money_helper_preserves_decimal_contract(orders_plan, orders_transform_text) -> None:
    """Reusable expression helpers preserve repeated field contracts."""

    projection = {assignment.field.name: assignment.expression for assignment in orders_plan.steps[0].projection}

    assert projection["total"].data == {"function": "coalesce"}
    assert projection["discount"].data == {"function": "coalesce"}
    assert projection["total"].args[0].data == {"function": "to_decimal", "precision": 12, "scale": 2}
    assert projection["discount"].args[0].data == {"function": "to_decimal", "precision": 12, "scale": 2}
    assert 'F.col("order_raw.total").cast("decimal(12,2)")' in orders_transform_text
    assert 'F.col("order_raw.discount").cast("decimal(12,2)")' in orders_transform_text
