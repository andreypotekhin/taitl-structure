def test_multiple_where_calls_remain_ordered_filter_predicates(orders_recipe) -> None:
    """I can call where(...) multiple times."""

    normalize = orders_recipe.steps[0]

    assert [predicate.kind for predicate in normalize.filters] == ["is_not_null", "is_not_null", "is_not_null"]
    assert [predicate.args[0].data for predicate in normalize.filters] == [
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
