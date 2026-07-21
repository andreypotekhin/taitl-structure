from typing import cast

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark import *


class Order(Schema):
    id = string(nullable=False)
    product_id = string(nullable=False)


class Product(Schema):
    id = string(nullable=False)
    name = string(nullable=False)


class Enriched(Schema):
    id = string(nullable=False)
    product_name = string(nullable=True)


def test_multiple_schema_parameters_and_results_are_explicit() -> None:
    """Developers can bind multiple schema parameters and tuple results in declaration order."""

    @transform
    class AddProduct(Transform):
        external = input(Order)
        products = input(Product)
        accepted = output(Enriched)
        audited = output(Enriched)

        @step(input=[external, products], output=[accepted, audited])
        def add_product(
            self,
            order: Order,
            product: Product,
        ) -> tuple[Enriched, Enriched]:
            product = lookup_join(
                product,
                on=product.id == order.product_id,
                how=Join.LEFT,
            )
            row = Enriched(id=order.id, product_name=product.name)
            return row, row

    plan = cast(TransformPlan, Compiler.frontend.compile()(AddProduct, materialize_schemas=False).analysis)

    assert [item.parameter for item in plan.steps[0].inputs] == ["order", "product"]
    assert [item.lane for item in plan.steps[0].results] == ["accepted", "audited"]
