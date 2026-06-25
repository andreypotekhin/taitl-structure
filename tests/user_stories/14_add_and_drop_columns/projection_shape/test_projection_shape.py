from testing.model.v1.orders.schemas.order import OrderPublished, OrderWithPromotion


def test_added_columns_are_declared_by_larger_output_schema(orders_plan) -> None:
    """I can add columns by returning an output schema with more fields than the input schema."""

    add_customer = orders_plan.steps[1]

    assert [assignment.field.name for assignment in add_customer.projection][-3:] == [
        "customer_name",
        "customer_tier",
        "customer_region",
    ]
    assert add_customer.output_schema._structure_fields.keys() > add_customer.input_schema._structure_fields.keys()


def test_dropped_columns_are_removed_by_output_projection(orders_plan) -> None:
    """I can drop columns by returning an output schema with fewer fields than the input schema."""

    publish = orders_plan.steps[-1]
    published_fields = [assignment.field.name for assignment in publish.projection]

    assert publish.input_schema is OrderWithPromotion
    assert publish.output_schema is OrderPublished
    assert "audit" not in published_fields
    assert "product_id" not in published_fields
    assert "promotion_discount" not in published_fields
    assert published_fields == list(OrderPublished._structure_fields)


def test_generated_projection_uses_select_instead_of_dataframe_drop(orders_transform_text) -> None:
    """I can rely on generated projection rather than Spark drop(...) so output schema is deterministic."""

    assert '        published = orders.alias("order_with_promotion")' in orders_transform_text
    assert "        published = published.select(" in orders_transform_text
    assert 'F.col("order_with_promotion.product_name").alias("product_name")' in orders_transform_text
    assert 'F.col("order_with_promotion.product_id").alias("product_id")' not in orders_transform_text
    assert ".drop(" not in orders_transform_text
