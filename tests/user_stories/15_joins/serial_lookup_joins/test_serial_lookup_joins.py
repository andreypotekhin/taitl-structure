from structure.app.dsl.api import Join, JoinHint


def test_serial_lookup_joins_record_explicit_sources_types_and_hints(orders_recipe) -> None:
    """I can perform serial joins across an arbitrary number of inputs."""

    customer = orders_recipe.steps[1].joins[0]
    product = orders_recipe.steps[2].joins[0]
    promotion = orders_recipe.steps[3].joins[0]

    assert [(join.input_name, join.source, join.how, join.hint) for join in [customer, product, promotion]] == [
        ("customers", "customers", Join.LEFT, JoinHint.BROADCAST),
        ("product", "products", Join.LEFT, None),
        ("promotions", "promotions", Join.LEFT, None),
    ]
    assert customer.predicate.kind == "and"
    assert product.predicate.kind == "and"
    assert promotion.predicate.args[1].kind == "null_safe_eq"


def test_generated_joins_use_named_dataframe_inputs_and_no_stringly_source_fields(orders_transform_text) -> None:
    """I can express joins symbolically using input scopes."""

    assert 'customers_joined = F.broadcast(customers.alias("customers"))' in orders_transform_text
    assert 'products_joined = products.alias("products")' in orders_transform_text
    assert 'promotions_joined = promotions.alias("promotions")' in orders_transform_text
    assert 'F.col("customers.tenant.tenant_id") == F.col("order_normalized.tenant.tenant_id")' in orders_transform_text
    assert 'F.col("products.id") == F.col("order_with_customer.product_id")' in orders_transform_text
    assert "eqNullSafe" in orders_transform_text
