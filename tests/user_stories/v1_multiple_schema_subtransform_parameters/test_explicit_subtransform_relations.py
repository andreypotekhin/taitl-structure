from structure import Join, String, Structure, Transform, field, input, join_one, output, transform
from structure.app.dsl.api import compile_transform


class Order(Structure):
    id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)


class Product(Structure):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=False)


class Enriched(Structure):
    id = field(String(), nullable=False)
    product_name = field(String(), nullable=True)


def test_multiple_schema_parameters_and_results_are_explicit() -> None:
    """Developers can bind multiple schema parameters and tuple results in declaration order."""

    @transform
    class AddProduct(Transform):
        external = input(Order)
        products = input(Product)
        accepted = output(Enriched)
        audited = output(Enriched)

        @transform(inputs=[external, products], outputs=[accepted, audited])
        def add_product(
            self,
            order: Order,
            product: Product,
        ) -> tuple[Enriched, Enriched]:
            product = join_one(
                product,
                on=product.id == order.product_id,
                how=Join.LEFT,
            )
            row = Enriched(id=order.id, product_name=product.name)
            return row, row

    plan = compile_transform(AddProduct)

    assert [item.parameter for item in plan.steps[0].inputs] == ["order", "product"]
    assert [item.lane for item in plan.steps[0].results] == ["accepted", "audited"]
