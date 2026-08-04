import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark import PySpark


@pytest.fixture
def orders_plan():
    from testing.model.orders.transforms.order import EnrichOrders

    return Compiler.frontend.compile()(EnrichOrders, materialize_schemas=False).analysis


@pytest.fixture
def orders_recipe():
    from testing.model.orders.transforms.order import EnrichOrders

    return Compiler.frontend.compile()(EnrichOrders, materialize_schemas=False).lowered


@pytest.fixture
def orders_transform_text(orders_recipe) -> str:
    from testing.model.orders.schemas.customer import Customer
    from testing.model.orders.schemas.order import (
        OrderFulfillment,
        OrderNormalized,
        OrderPublished,
        OrderRaw,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
    )
    from testing.model.orders.schemas.product import BlockedProduct, Product
    from testing.model.orders.schemas.promotion import Promotion
    from testing.model.orders.schemas.shipment import Shipment

    order_module = "testing.model.structure_generated.orders.pyspark.schemas.order"
    return PySpark.render.transform()(
        orders_recipe,
        source_transform="testing.model.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.structure_generated.orders.runtime.schema_assert",
        schema_modules={
            OrderRaw: order_module,
            OrderNormalized: order_module,
            OrderWithCustomer: order_module,
            OrderWithProduct: order_module,
            OrderWithPromotion: order_module,
            OrderFulfillment: order_module,
            OrderPublished: order_module,
            Customer: "testing.model.structure_generated.orders.pyspark.schemas.customer",
            Product: "testing.model.structure_generated.orders.pyspark.schemas.product",
            BlockedProduct: "testing.model.structure_generated.orders.pyspark.schemas.product",
            Promotion: "testing.model.structure_generated.orders.pyspark.schemas.promotion",
            Shipment: "testing.model.structure_generated.orders.pyspark.schemas.shipment",
        },
    )


@pytest.fixture
def orders_traceability(orders_recipe):
    return Compiler.traceability.build()(
        orders_recipe,
        source_transform="testing.model.orders.transforms.order.EnrichOrders",
        transform_module="testing.model.structure_generated.orders.pyspark.transforms.testing.model.orders.transforms.order",
    )
