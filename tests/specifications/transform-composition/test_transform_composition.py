from typing import Any, cast

import pytest

from structure import *
from structure.core.runtime.session.model.TransformResult import TransformResult
from structure.platform.pyspark.api import PySpark


class Raw(Schema):
    id = field.string(nullable=False)
    product_id = field.string(nullable=True)


class Normalized(Schema):
    id = field.string(nullable=False)
    product_id = field.string(nullable=True)


class Product(Schema):
    id = field.string(nullable=False)
    name = field.string(nullable=True)


class Enriched(Schema):
    id = field.string(nullable=False)
    product_name = field.string(nullable=True)


class Published(Schema):
    id = field.string(nullable=False)
    product_name = field.string(nullable=True)


class Metric(Schema):
    id = field.string(nullable=False)
    value = field.integer(nullable=True)


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


def test_output_alias_satisfies_downstream_input_name() -> None:
    @transform
    class NormalizeWithBoundaryAlias(Transform):
        orders = input(Raw)
        normalized = output(Normalized).alias("orders")

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, product_id=row.product_id)

    @transform
    class PublishNormalized(Transform):
        orders = input(Normalized)
        published = output(Published)

        def publish(self, order: Normalized) -> Published:
            return Published(id=order.id, product_name=order.product_id)

    plan = compile_transform(NormalizeWithBoundaryAlias(orders=object()).to(PublishNormalized()))

    assert [input.name for input in plan.inputs] == ["orders"]
    assert [step.name for step in plan.steps] == [
        "normalize_with_boundary_alias.normalize",
        "publish_normalized.publish",
    ]


def test_stage_rename_satisfies_downstream_input_name() -> None:
    @transform
    class PublishNormalized(Transform):
        orders = input(Normalized)
        published = output(Published)

        def publish(self, order: Normalized) -> Published:
            return Published(id=order.id, product_name=order.product_id)

    plan = compile_transform(NormalizeOrders(orders=object()).rename(normalized="orders").to(PublishNormalized()))

    assert [input.name for input in plan.inputs] == ["orders"]
    assert [output.name for output in plan.outputs] == ["published"]


def test_input_alias_satisfies_upstream_output_name() -> None:
    @transform
    class PublishNormalized(Transform):
        rows = input(Normalized).alias("normalized")
        published = output(Published)

        def publish(self, row: Normalized) -> Published:
            return Published(id=row.id, product_name=row.product_id)

    plan = compile_transform(NormalizeOrders(orders=object()).to(PublishNormalized()))

    assert [input.name for input in plan.inputs] == ["orders"]


def test_ambiguous_output_alias_match_fails() -> None:
    with pytest.raises(TypeError, match="output alias orders is used by both accepted and rejected"):

        @transform
        class RouteMetrics(Transform):
            rows = input(Metric)
            accepted = output(Metric).alias("orders")
            rejected = output(Metric).alias("orders")

            @step(output=[accepted, rejected])
            def route(self, row: Metric) -> tuple[Metric, Metric]:
                return (
                    Metric(id=row.id, value=row.value),
                    Metric(id=row.id, value=row.value),
                )


def test_input_alias_constructor_keyword_normalizes_to_canonical_input() -> None:
    @transform
    class NormalizeInputAlias(Transform):
        rows = input(Raw).alias("orders")
        normalized = output(Normalized)

        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, product_id=row.product_id)

    frame = object()
    invocation = NormalizeInputAlias(orders=frame)

    assert invocation._structure_bound_inputs == {"rows": frame}
    with pytest.raises(TypeError, match="more than once"):
        NormalizeInputAlias(rows=frame, orders=frame)


def test_result_aliases_are_lookup_synonyms_not_mapping_keys() -> None:
    frame = object()
    result = TransformResult(
        {"normalized": frame},
        single=True,
        schema={"normalized": "schema"},
        aliases={"normalized": ("orders",)},
    )

    assert result.normalized is frame
    assert result.orders is frame
    assert result["orders"] is frame
    assert result.schema.orders == "schema"
    assert result.schema["orders"] == "schema"
    assert list(result) == ["normalized"]
    assert result.as_dict() == {"normalized": frame}


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

        @step(output=[accepted, rejected])
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
        schema_modules={
            Raw: __name__,
            Normalized: __name__,
            Product: __name__,
            Enriched: __name__,
            Published: __name__,
        },
    )

    assert [input.name for input in plan.inputs] == ["orders", "products"]
    assert [output.name for output in plan.outputs] == ["published"]
    assert text.index("# Step method: normalize_orders.normalize") < text.index(
        "# Step method: add_product.add_product"
    )
    assert text.index("# Step method: add_product.add_product") < text.index("# Step method: publish_orders.publish")


def test_generated_transform_renders_output_alias_metadata() -> None:
    @transform
    class NormalizeWithBoundaryAlias(Transform):
        orders = input(Raw)
        normalized = output(Normalized).alias("orders")

        def normalize(self, order: Raw) -> Normalized:
            return Normalized(id=order.id, product_id=order.product_id)

    plan = compile_transform(NormalizeWithBoundaryAlias)
    text = PySpark.render.transform()(
        PySpark.plan.lower()(plan),
        source_transform=f"{__name__}.NormalizeWithBoundaryAlias",
        runtime_module="testing.model.v1.structure_generated.runtime.schema_assert",
        schema_modules={Raw: __name__, Normalized: __name__},
    )

    assert 'aliases={\'normalized\': (\'orders\',)}' in text


def test_inherited_lane_remains_available_to_override() -> None:
    class BaseNormalize(Transform):
        rows = input(Raw)
        normalized = lane(Normalized)

        @step(output=normalized)
        def normalize(self, row: Raw) -> Normalized:
            return Normalized(id=row.id, product_id=row.product_id)

    @transform
    class Publish(BaseNormalize):
        published = output(Normalized)

        @step(output=BaseNormalize.normalized)
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
