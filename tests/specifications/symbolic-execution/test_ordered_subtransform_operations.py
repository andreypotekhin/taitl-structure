from typing import Any, cast

import pytest

from structure import (
    Join,
    String,
    Structure,
    StructureCompileError,
    Transform,
    field,
    input,
    join_one,
    output,
    project,
    transform,
    where,
)
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


class Order(Structure):
    id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)
    status = field(String(), nullable=True)


class Product(Structure):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=False)


class Published(Structure):
    id = field(String(), nullable=False)
    status = field(String(), nullable=True)


class Enriched(Structure):
    id = field(String(), nullable=False)
    product_name = field(String(), nullable=True)


def test_where_before_join_renders_before_join() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            where(cast(Any, order.status).is_not_null())
            join_one(product, on=product.id == order.product_id, how=Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    step = PySpark.plan.lower()(compile_transform(AddProduct)).steps[0]
    text = PySpark.render.step()(step, current="orders", sources={"products": "products"})

    assert text.index("orders = orders.where(") < text.index("orders = orders.join(")


def test_where_after_join_renders_after_join() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            join_one(product, on=product.id == order.product_id, how=Join.LEFT)
            where(cast(Any, product).name.is_not_null())
            return Enriched(id=order.id, product_name=product.name)

    step = PySpark.plan.lower()(compile_transform(AddProduct)).steps[0]
    text = PySpark.render.step()(step, current="orders", sources={"products": "products"})

    assert text.index("orders = orders.join(") < text.index("orders = orders.where(")


def test_bare_join_one_makes_later_relation_reads_joined() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            join_one(product, on=product.id == order.product_id, how=Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    plan = compile_transform(AddProduct)
    projection = {assignment.field.name: assignment.expression for assignment in plan.steps[0].projection}
    product_name = projection["product_name"]
    product_name_data = cast(dict[str, object], product_name.data)

    assert plan.steps[0].joins[0].source == "products"
    assert product_name_data["scope"] == "product"
    assert product_name.nullable


def test_pre_join_relation_filter_still_fails() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            where(cast(Any, product).name.is_not_null())
            join_one(product, on=product.id == order.product_id, how=Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "reads relation parameter product before it is joined" in raised.value.diagnostic.problem_text()


def test_source_less_project_uses_driving_row() -> None:
    @transform
    class Publish(Transform):
        orders = input(Order)
        published = output(Published)

        def publish(self, order: Order) -> Published:
            return project(Published)

    plan = compile_transform(Publish)

    assert [assignment.field.name for assignment in plan.steps[0].projection] == ["id", "status"]


def test_return_chain_join_where_project_uses_ordered_operations() -> None:
    @transform
    class Publish(Transform):
        orders = input(Order)
        products = input(Product)
        published = output(Published)

        def publish(self, order: Order, product: Product) -> Published:
            return cast(Any, join_one(product, on=product.id == order.product_id)).where(
                cast(Any, order).status.is_not_null()
            ).project(Published)

    step = compile_transform(Publish).steps[0]

    assert [operation.kind for operation in step.operations] == ["join", "filter"]
    assert [assignment.field.name for assignment in step.projection] == ["id", "status"]
