def test_multiple_where_calls_remain_ordered_filter_predicates(orders_recipe) -> None:
    """I can call where(...) multiple times."""

    normalize = orders_recipe.steps[0]

    assert [predicate.kind for predicate in normalize.filters] == ["is_not_null", "is_not_null", "is_not_null"]
    assert [_field(predicate.args[0].data) for predicate in normalize.filters] == [
        {"scope": "orders", "field": "id"},
        {"scope": "orders", "field": "customer_id"},
        {"scope": "orders", "field": "product_id"},
    ]


def test_generated_filtering_uses_dataframe_where_with_optimizer_visible_columns(orders_transform_text) -> None:
    """I can call where(predicate) inside a subtransform."""

    assert "        orders = orders.where(" in orders_transform_text
    assert 'F.col("order_raw.id").isNotNull()' in orders_transform_text
    assert 'F.col("order_raw.customer_id").isNotNull()' in orders_transform_text
    assert 'F.col("order_raw.product_id").isNotNull()' in orders_transform_text
    assert " & " in orders_transform_text


def _field(data):
    return {key: data[key] for key in ("scope", "field")}
