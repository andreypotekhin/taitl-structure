from typing import Any, cast

import pytest

from structure import (
    Integer,
    String,
    Structure,
    StructureCompileError,
    StructureSession,
    Transform,
    field,
    input,
    lane,
    output,
    transform,
    where,
)
from structure.app.dsl.api import compile_transform
from structure.app.runtime.session.model.TransformResult import TransformResult
from structure.app.target.pyspark.api import PySpark


class Raw(Structure):
    id = field(String(), nullable=False)
    product_id = field(String(), nullable=True)


class Normalized(Structure):
    id = field(String(), nullable=False)
    product_id = field(String(), nullable=True)


class Product(Structure):
    id = field(String(), nullable=False)
    name = field(String(), nullable=True)


class Enriched(Structure):
    id = field(String(), nullable=False)
    product_name = field(String(), nullable=True)


class Published(Structure):
    id = field(String(), nullable=False)
    product_name = field(String(), nullable=True)


class Metric(Structure):
    id = field(String(), nullable=False)
    value = field(Integer(), nullable=True)


@transform
class NormalizeOrders(Transform):
    orders = input(Raw)
    normalized = output(Normalized)

    def normalize(self, order: Raw) -> Normalized:
        return Normalized(id=order.id, product_id=order.product_id)


@transform
class AddProduct(Transform):
    normalized = input(Normalized)
    products = input(Product)
    enriched = output(Enriched)

    def add_product(self, order: Normalized, product: Product) -> Enriched:
        return Enriched(id=order.id, product_name=order.product_id)


@transform
class PublishOrders(Transform):
    enriched = input(Enriched)
    published = output(Published)

    def publish(self, order: Enriched) -> Published:
        return Published(id=order.id, product_name=order.product_name)


def test_instance_to_runs_with_final_output_shape() -> None:
    captured = {}

    def executor(**kwargs):
        captured["steps"] = [step.name for step in kwargs["plan"].steps]
        return object()

    result = (
        NormalizeOrders(orders=object())
        .to(AddProduct(products=object()))
        .to(PublishOrders())
        .run(StructureSession(schema_types=FakeTypes, online_executor=executor))
    )

    assert isinstance(result, TransformResult)
    assert result.published is not None
    assert captured["steps"] == [
        "normalize_orders.normalize",
        "add_product.add_product",
        "publish_orders.publish",
    ]


def test_multi_argument_to_matches_sequential_to() -> None:
    product = object()
    multi = NormalizeOrders(orders=object()).to(AddProduct(products=product), PublishOrders())
    sequential = NormalizeOrders(orders=object()).to(AddProduct(products=product)).to(PublishOrders())

    assert [step.name for step in compile_transform(multi).steps] == [
        step.name for step in compile_transform(sequential).steps
    ]


def test_static_transform_to_starts_pipeline() -> None:
    pipeline = Transform.to(NormalizeOrders(orders=object()), AddProduct(products=object()), PublishOrders())

    assert [step.name for step in compile_transform(pipeline).steps] == [
        "normalize_orders.normalize",
        "add_product.add_product",
        "publish_orders.publish",
    ]


def test_downstream_constructor_input_satisfies_missing_input() -> None:
    plan = compile_transform(NormalizeOrders(orders=object()).to(AddProduct(products=object())))

    assert [input.name for input in plan.inputs] == ["orders", "products"]
    assert [output.name for output in plan.outputs] == ["enriched"]


def test_downstream_constructor_conflicts_with_matching_upstream_output() -> None:
    pipeline = NormalizeOrders(orders=object()).to(AddProduct(normalized=object(), products=object()))

    with pytest.raises(StructureCompileError, match="both explicitly bound and produced upstream"):
        compile_transform(pipeline)


def test_missing_downstream_input_fails() -> None:
    pipeline = NormalizeOrders(orders=object()).to(AddProduct())

    with pytest.raises(StructureCompileError, match="products is not supplied"):
        compile_transform(pipeline)


def test_ambiguous_upstream_output_match_fails() -> None:
    @transform
    class RouteMetrics(Transform):
        rows = input(Metric)
        accepted = output(Metric)
        rejected = output(Metric)

        @transform(output=[accepted, rejected])
        def route(self, row: Metric) -> tuple[Metric, Metric]:
            return (
                Metric(id=row.id, value=row.value),
                Metric(id=row.id, value=row.value),
            )

    @transform
    class PublishMetric(Transform):
        row = input(Metric)
        published = output(Metric)

        def publish(self, row: Metric) -> Metric:
            return Metric(id=row.id, value=row.value)

    pipeline = RouteMetrics(rows=object()).to(PublishMetric())

    with pytest.raises(StructureCompileError, match="matched outputs: accepted, rejected"):
        compile_transform(pipeline)


def test_lane_declaration_cannot_be_constructor_binding() -> None:
    class LaneOwner(Transform):
        rows = lane(Raw)

    pipeline = Transform.to(NormalizeOrders(orders=LaneOwner.rows))

    with pytest.raises(StructureCompileError, match="bound to a lane"):
        compile_transform(pipeline)


def test_class_field_pipeline_compiles_and_renders_generated_transform() -> None:
    @transform
    class OrderPipeline(Transform):
        orders = input(Raw)
        products = input(Product)

        pipeline = Transform.to(
            NormalizeOrders(orders=orders),
            AddProduct(products=products),
            PublishOrders(),
        )

    plan = compile_transform(OrderPipeline)
    text = PySpark.render.transform()(
        PySpark.plan.lower()(plan),
        source_transform=f"{__name__}.OrderPipeline",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules={Raw: __name__, Normalized: __name__, Product: __name__, Enriched: __name__, Published: __name__},
    )

    assert [input.name for input in plan.inputs] == ["orders", "products"]
    assert [output.name for output in plan.outputs] == ["published"]
    assert text.index("# Subtransform: normalize_orders.normalize") < text.index(
        "# Subtransform: add_product.add_product"
    )
    assert text.index("# Subtransform: add_product.add_product") < text.index(
        "# Subtransform: publish_orders.publish"
    )


def test_inherited_lane_remains_available_to_override() -> None:
    class BaseNormalize(Transform):
        rows = input(Raw)
        normalized = lane(Normalized)

        @transform(output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, product_id=row.product_id)

    @transform
    class Publish(BaseNormalize):
        published = output(Normalized)

        @transform(output=BaseNormalize.normalized)
        def normalize(self, row: Raw) -> Normalized:
            where(cast(Any, row.id).is_not_null())
            return Normalized(id=row.id, product_id=row.product_id)

        def publish(self, row: Normalized) -> Normalized:
            return Normalized(id=row.id, product_id=row.product_id)

    plan = compile_transform(Publish)

    assert [step.name for step in plan.steps] == ["normalize", "publish"]
    assert len(plan.steps[0].filters) == 1


class FakeTypes:
    @staticmethod
    def StructType(fields):
        return ("StructType", tuple(fields))

    @staticmethod
    def StructField(name, dataType, nullable):
        return ("StructField", name, dataType, nullable)

    @staticmethod
    def StringType():
        return "StringType"

    @staticmethod
    def IntegerType():
        return "IntegerType"
