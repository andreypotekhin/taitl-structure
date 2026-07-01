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
from structure.app.compiler.api import OperationCardinality, StreamingSupport
from structure.app.compiler.ir.model.JoinMethod import JoinMethod
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


def test_bare_inferred_join_one_makes_later_relation_reads_joined() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            join_one(on=product.id == order.product_id, how=Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    step = compile_transform(AddProduct).steps[0]
    projection = {assignment.field.name: assignment.expression for assignment in step.projection}
    product_name = projection["product_name"]
    product_name_data = cast(dict[str, object], product_name.data)

    assert step.joins[0].source == "products"
    assert step.operations[0].kind == "join"
    assert step.operations[0].join == step.joins[0]
    assert step.operations[0].capability is not None
    assert step.operations[0].capability.group == "join"
    assert step.operations[0].capability.name == "join_one"
    assert step.operations[0].cardinality is OperationCardinality.SELECT_ONE
    assert product_name_data["scope"] == "product"
    assert product_name.nullable


def test_inferred_join_one_preserves_filter_join_order() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            where(cast(Any, order.status).is_not_null())
            join_one(on=product.id == order.product_id, how=Join.LEFT)
            where(cast(Any, product).name.is_not_null())
            return Enriched(id=order.id, product_name=product.name)

    step = compile_transform(AddProduct).steps[0]

    assert [operation.kind for operation in step.operations] == ["filter", "join", "filter"]
    assert [operation.cardinality for operation in step.operations] == [
        OperationCardinality.ROW_FILTERING,
        OperationCardinality.SELECT_ONE,
        OperationCardinality.ROW_FILTERING,
    ]
    assert [operation.streaming for operation in step.operations] == [
        StreamingSupport.COMPATIBLE,
        StreamingSupport.UNKNOWN,
        StreamingSupport.COMPATIBLE,
    ]

    recipe = PySpark.plan.lower()(compile_transform(AddProduct)).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert text.index("orders = orders.where(") < text.index("orders = orders.join(")
    assert text.rindex("orders = orders.where(") > text.index("orders = orders.join(")


def test_exists_join_records_row_filtering_operation() -> None:
    @transform
    class PublishKnownProducts(Transform):
        orders = input(Order)
        products = input(Product)
        published = output(Published)

        def publish(self, order: Order, product: Product) -> Published:
            where(cast(Any, product).exists(on=product.id == order.product_id))
            return Published(id=order.id, status=order.status)

    step = compile_transform(PublishKnownProducts).steps[0]

    assert len(step.joins) == 1
    assert step.joins[0].method is JoinMethod.EXISTS
    assert [operation.kind for operation in step.operations] == ["join"]
    assert step.operations[0].capability is not None
    assert step.operations[0].capability.name == "exists"
    assert step.operations[0].cardinality is OperationCardinality.ROW_FILTERING

    recipe = PySpark.plan.lower()(compile_transform(PublishKnownProducts)).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert '"left_semi"' in text
    assert "orders = orders.where(" not in text


def test_not_exists_join_records_row_filtering_operation() -> None:
    @transform
    class PublishUnknownProducts(Transform):
        orders = input(Order)
        products = input(Product)
        published = output(Published)

        def publish(self, order: Order, product: Product) -> Published:
            where(cast(Any, product).not_exists(on=product.id == order.product_id))
            return Published(id=order.id, status=order.status)

    step = compile_transform(PublishUnknownProducts).steps[0]
    recipe = PySpark.plan.lower()(compile_transform(PublishUnknownProducts)).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert step.joins[0].method is JoinMethod.NOT_EXISTS
    assert step.operations[0].capability is not None
    assert step.operations[0].capability.name == "not_exists"
    assert step.operations[0].cardinality is OperationCardinality.ROW_FILTERING
    assert '"left_anti"' in text


def test_exists_join_does_not_make_relation_fields_readable() -> None:
    @transform
    class PublishKnownProducts(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def publish(self, order: Order, product: Product) -> Enriched:
            where(cast(Any, product).exists(on=product.id == order.product_id))
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(PublishKnownProducts)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "reads relation parameter product before it is joined" in raised.value.diagnostic.problem_text()


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
            return (
                cast(Any, join_one(product, on=product.id == order.product_id))
                .where(cast(Any, order).status.is_not_null())
                .project(Published)
            )

    step = compile_transform(Publish).steps[0]

    assert [operation.kind for operation in step.operations] == ["join", "filter"]
    assert [assignment.field.name for assignment in step.projection] == ["id", "status"]
