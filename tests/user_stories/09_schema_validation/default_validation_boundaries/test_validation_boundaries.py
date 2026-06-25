from testing.model.v1.orders.schemas.order import (
    OrderNormalized,
    OrderPublished,
    OrderWithCustomer,
    OrderWithProduct,
    OrderWithPromotion,
)

from structure.app.dsl.api import SchemaMode


def test_intermediate_schema_validation_is_placed_after_subtransforms(orders_recipe) -> None:
    """I can have intermediate schemas validated after each subtransform by default."""

    assert [
        (step.name, step.validations[-1].schema, step.validations[-1].mode, step.validations[-1].reason)
        for step in orders_recipe.steps[:4]
    ] == [
        ("normalize", OrderNormalized, SchemaMode.STRICT, "intermediate"),
        ("add_customer", OrderWithCustomer, SchemaMode.STRICT, "intermediate"),
        ("add_product", OrderWithProduct, SchemaMode.STRICT, "intermediate"),
        ("add_promotion", OrderWithPromotion, SchemaMode.STRICT, "intermediate"),
    ]


def test_hook_extra_columns_are_allowed_only_until_output_projection(orders_recipe) -> None:
    """Hook extra columns are allowed only until output projection."""

    publish_validations = orders_recipe.steps[-1].validations

    assert [
        (validation.schema, validation.mode, validation.project, validation.reason)
        for validation in publish_validations
    ] == [
        (OrderPublished, SchemaMode.ALLOW_EXTRA_COLUMNS, True, "hook"),
        (OrderPublished, SchemaMode.STRICT, False, "hook_projected"),
    ]
    assert orders_recipe.final_validation.schema is OrderPublished
    assert orders_recipe.final_validation.mode is SchemaMode.STRICT
    assert orders_recipe.final_validation.reason == "final"


def test_generated_code_asserts_input_intermediate_and_final_schemas(orders_transform_text) -> None:
    """I can have final output schema validation enabled by default so generated outputs conform to their declared contract."""

    assert 'assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")' in orders_transform_text
    assert (
        'assert_schema(orders, ORDER_NORMALIZED_SCHEMA, name="OrderNormalized", mode="strict")' in orders_transform_text
    )
    assert (
        'assert_schema(orders, ORDER_WITH_CUSTOMER_SCHEMA, name="OrderWithCustomer", mode="strict")'
        in orders_transform_text
    )
    assert (
        'assert_schema(orders, ORDER_WITH_PRODUCT_SCHEMA, name="OrderWithProduct", mode="strict")'
        in orders_transform_text
    )
    assert (
        'assert_schema(orders, ORDER_WITH_PROMOTION_SCHEMA, name="OrderWithPromotion", mode="strict")'
        in orders_transform_text
    )
    assert (
        'assert_schema(published, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="strict")'
        in orders_transform_text
    )
