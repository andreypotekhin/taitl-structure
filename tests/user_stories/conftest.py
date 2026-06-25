import pytest

from structure.app.compiler.api import Compiler
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


@pytest.fixture
def orders_plan():
    from testing.model.v1.orders.transforms.order import EnrichOrders

    return compile_transform(EnrichOrders)


@pytest.fixture
def orders_recipe(orders_plan):
    return PySpark.plan.lower()(orders_plan)


@pytest.fixture
def orders_transform_text(orders_recipe) -> str:
    from testing.model.v1.orders.schemas.customer import Customer
    from testing.model.v1.orders.schemas.order import (
        OrderNormalized,
        OrderPublished,
        OrderRaw,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
    )
    from testing.model.v1.orders.schemas.product import Product
    from testing.model.v1.orders.schemas.promotion import Promotion

    order_module = "testing.model.v1.structure_generated.orders.pyspark.schemas.order"
    return PySpark.render.transform()(
        orders_recipe,
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        runtime_module="testing.model.v1.structure_generated.orders.runtime.schema_assert",
        schema_modules={
            OrderRaw: order_module,
            OrderNormalized: order_module,
            OrderWithCustomer: order_module,
            OrderWithProduct: order_module,
            OrderWithPromotion: order_module,
            OrderPublished: order_module,
            Customer: "testing.model.v1.structure_generated.orders.pyspark.schemas.customer",
            Product: "testing.model.v1.structure_generated.orders.pyspark.schemas.product",
            Promotion: "testing.model.v1.structure_generated.orders.pyspark.schemas.promotion",
        },
    )


@pytest.fixture
def orders_traceability(orders_recipe):
    return Compiler.traceability.build()(
        orders_recipe,
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        transform_module="testing.model.v1.structure_generated.orders.pyspark.transforms.order",
    )
