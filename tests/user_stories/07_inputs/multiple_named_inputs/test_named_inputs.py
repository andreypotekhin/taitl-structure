from testing.model.orders.schemas.customer import Customer
from testing.model.orders.schemas.order import OrderRaw
from testing.model.orders.schemas.product import BlockedProduct, Product
from testing.model.orders.schemas.promotion import Promotion
from testing.model.orders.schemas.shipment import Shipment

from structure import *
from structure.plugin.pyspark import *


def test_declared_inputs_keep_names_schemas_and_order(orders_plan) -> None:
    """I can declare multiple named inputs."""

    assert [(item.name, item.schema, item.ordinal) for item in orders_plan.inputs] == [
        ("orders", OrderRaw, 0),
        ("customers", Customer, 1),
        ("products", Product, 2),
        ("blocked_products", BlockedProduct, 3),
        ("promotions", Promotion, 4),
        ("shipments", Shipment, 5),
    ]


def test_generated_entrypoint_uses_named_keyword_dataframe_parameters(orders_transform_text) -> None:
    """I can declare multiple named inputs so that generated run(...) uses named keyword arguments."""

    assert "    def run(\n        self,\n        *,\n" in orders_transform_text
    assert "        orders: DataFrame," in orders_transform_text
    assert "        customers: DataFrame," in orders_transform_text
    assert "        products: DataFrame," in orders_transform_text
    assert "        blocked_products: DataFrame," in orders_transform_text
    assert "        promotions: DataFrame," in orders_transform_text
    assert "        shipments: DataFrame," in orders_transform_text


def test_input_dataframe_validation_is_bound_to_declared_schema(orders_recipe) -> None:
    """I can validate input DataFrames against declared schemas."""

    assert [(item.name, item.validation.schema, item.validation.mode) for item in orders_recipe.inputs] == [
        ("orders", OrderRaw, SchemaMode.STRICT),
        ("customers", Customer, SchemaMode.STRICT),
        ("products", Product, SchemaMode.STRICT),
        ("blocked_products", BlockedProduct, SchemaMode.STRICT),
        ("promotions", Promotion, SchemaMode.STRICT),
        ("shipments", Shipment, SchemaMode.STRICT),
    ]
    assert [item.validation.reason for item in orders_recipe.inputs] == ["input"] * 6
