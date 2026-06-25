from testing.model.v1.orders.schemas.order import (
    OrderNormalized,
    OrderPublished,
    OrderRaw,
    OrderWithCustomer,
    OrderWithProduct,
    OrderWithPromotion,
)


def test_public_schema_methods_compile_in_source_order(orders_plan) -> None:
    """I can rely on source order for subtransform execution."""

    assert [(step.name, step.input_schema, step.output_schema) for step in orders_plan.steps] == [
        ("normalize", OrderRaw, OrderNormalized),
        ("add_customer", OrderNormalized, OrderWithCustomer),
        ("add_product", OrderWithCustomer, OrderWithProduct),
        ("add_promotion", OrderWithProduct, OrderWithPromotion),
        ("publish", OrderWithPromotion, OrderPublished),
    ]


def test_named_output_field_defines_final_result_lane(orders_plan) -> None:
    """I must declare at least one named output field."""

    assert [(output.name, output.schema) for output in orders_plan.outputs] == [("published", OrderPublished)]
    assert orders_plan.steps[-1].results[0].lane == "published"
