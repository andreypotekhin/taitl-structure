from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler, StreamingSupport
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.joins import JoinMethod
from structure.plugin.pyspark.dsl.operations import OperationCardinality
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def _compile(transform):
    return Compiler.frontend.compile()(transform, materialize_schemas=False)


def _analysis(transform):
    return _compile(transform).analysis


def _body(transform, step: int = 0) -> PySparkStepBody:
    return cast(PySparkStepBody, _analysis(transform).steps[step].plugin_body)


def _recipe(transform) -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, _compile(transform).lowered)


class Order(Schema):
    id = string(nullable=False)
    product_id = string(nullable=False)
    status = string(nullable=True)


class Product(Schema):
    id = string(nullable=False)
    name = string(nullable=False)
    valid_from = string(nullable=False)
    valid_to = string(nullable=True)


class Published(Schema):
    id = string(nullable=False)
    status = string(nullable=True)


class Enriched(Schema):
    id = string(nullable=False)
    product_name = string(nullable=True)


class OuterEnriched(Schema):
    id = string(nullable=True)
    product_name = string(nullable=True)


@pytest.mark.parametrize(
    ("argument", "message"),
    [
        ("how", r'lookup_join\(\.\.\.\) how= must be one of "left", "inner", "right", "full", "cross"'),
        ("hint", r'lookup_join\(\.\.\.\) hint= must be one of "broadcast"'),
    ],
)
def test_lookup_join_rejects_invalid_options_at_the_dsl_boundary(argument: str, message: str) -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            options = cast(Any, {argument: "invalid"})
            lookup_join(product, on=product.id == order.product_id, **options)
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError, match=message):
        _compile(AddProduct)


@pytest.mark.parametrize("ties", ["panic", None])
def test_join_dedupe_factory_rejects_invalid_tie_policy(ties: object) -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            lookup_join(
                product,
                on=product.id == order.product_id,
                dedupe=JoinDedupe.latest_by(product.name, ties=cast(Any, ties)),
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(TypeError, match=r'JoinDedupe.latest_by\(\.\.\.\) ties= must be one of "error"'):
        _compile(AddProduct)


@pytest.mark.parametrize(
    ("order_by", "message"),
    [
        (lambda product: product.name.desc(), "requires an unordered expression"),
        (lambda product: product.id.is_not_null(), "requires an orderable scalar expression"),
    ],
)
def test_join_dedupe_factory_rejects_invalid_ordering(order_by, message: str) -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            lookup_join(
                product,
                on=product.id == order.product_id,
                dedupe=JoinDedupe.latest_by(order_by(product)),
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError, match=message):
        _compile(AddProduct)


def test_join_dedupe_compiler_validation_rejects_manual_order_descriptor() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            lookup_join(
                product,
                on=product.id == order.product_id,
                dedupe=JoinDedupe(order_by=product.name.desc(), direction="latest"),
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError, match="must be an unordered expression"):
        _compile(AddProduct)


def test_selected_row_helpers_reject_order_descriptors() -> None:
    @transform
    class KeepLatest(Transform):
        orders = input(Order)
        enriched = output(Enriched)

        def keep_latest(self, order: Order) -> Enriched:
            latest_by(order.id.desc(), partition_by=order.product_id)
            return Enriched(id=order.id, product_name=order.product_id)

    with pytest.raises(StructureCompileError, match="unordered expression"):
        _compile(KeepLatest)


def test_where_before_join_renders_before_join() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            where(cast(Any, order.status).is_not_null())
            lookup_join(product, on=product.id == order.product_id, how=Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    compiled_step = _recipe(AddProduct).steps[0]
    text = PySpark.render.step()(compiled_step, current="orders", sources={"products": "products"})

    assert text.index("orders = orders.where(") < text.index("orders = orders.join(")


def test_where_after_join_renders_after_join() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            lookup_join(product, on=product.id == order.product_id, how=Join.LEFT)
            where(cast(Any, product).name.is_not_null())
            return Enriched(id=order.id, product_name=product.name)

    compiled_step = _recipe(AddProduct).steps[0]
    text = PySpark.render.step()(compiled_step, current="orders", sources={"products": "products"})

    assert text.index("orders = orders.join(") < text.index("orders = orders.where(")


def test_bare_lookup_join_makes_later_relation_reads_joined() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            lookup_join(product, on=product.id == order.product_id, how=Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    projection = {assignment.field.name: assignment.expression for assignment in _body(AddProduct).projection}
    product_name = projection["product_name"]
    product_name_data = cast(dict[str, object], product_name.data)

    assert _body(AddProduct).joins[0].source == "products"
    assert product_name_data["scope"] == "product"
    assert product_name.nullable


def test_bare_inferred_lookup_join_makes_later_relation_reads_joined() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            lookup_join(on=product.id == order.product_id, how=Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    compiled_step = _body(AddProduct)
    projection = {assignment.field.name: assignment.expression for assignment in compiled_step.projection}
    product_name = projection["product_name"]
    product_name_data = cast(dict[str, object], product_name.data)

    assert compiled_step.joins[0].source == "products"
    assert compiled_step.operations[0].kind == "join"
    assert compiled_step.operations[0].join == compiled_step.joins[0]
    assert compiled_step.operations[0].capability is not None
    assert compiled_step.operations[0].capability.group == "join"
    assert compiled_step.operations[0].capability.name == "lookup_join"
    assert compiled_step.operations[0].cardinality is OperationCardinality.SELECT_ONE
    assert product_name_data["scope"] == "product"
    assert product_name.nullable


def test_inferred_lookup_join_preserves_filter_join_order() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            where(cast(Any, order.status).is_not_null())
            lookup_join(on=product.id == order.product_id, how=Join.LEFT)
            where(cast(Any, product).name.is_not_null())
            return Enriched(id=order.id, product_name=product.name)

    compiled_step = _body(AddProduct)

    assert [operation.kind for operation in compiled_step.operations] == ["filter", "join", "filter"]
    assert [operation.cardinality for operation in compiled_step.operations] == [
        OperationCardinality.ROW_FILTERING,
        OperationCardinality.SELECT_ONE,
        OperationCardinality.ROW_FILTERING,
    ]
    assert [operation.streaming for operation in compiled_step.operations] == [
        StreamingSupport.COMPATIBLE,
        StreamingSupport.UNKNOWN,
        StreamingSupport.COMPATIBLE,
    ]

    recipe = _recipe(AddProduct).steps[0]
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
            where(exists(on=product.id == order.product_id))
            return Published(id=order.id, status=order.status)

    compiled_step = _body(PublishKnownProducts)

    assert len(compiled_step.joins) == 1
    assert compiled_step.joins[0].method is JoinMethod.EXISTS
    assert [operation.kind for operation in compiled_step.operations] == ["join"]
    assert compiled_step.operations[0].capability is not None
    assert compiled_step.operations[0].capability.name == "exists"
    assert compiled_step.operations[0].cardinality is OperationCardinality.ROW_FILTERING

    recipe = _recipe(PublishKnownProducts).steps[0]
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
            where(not_exists(on=product.id == order.product_id))
            return Published(id=order.id, status=order.status)

    compiled_step = _body(PublishUnknownProducts)
    recipe = _recipe(PublishUnknownProducts).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert compiled_step.joins[0].method is JoinMethod.NOT_EXISTS
    assert compiled_step.operations[0].capability is not None
    assert compiled_step.operations[0].capability.name == "not_exists"
    assert compiled_step.operations[0].cardinality is OperationCardinality.ROW_FILTERING
    assert '"left_anti"' in text


def test_inner_join_records_row_multiplying_operation() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            inner_join(
                on=product.id == order.product_id,
                strategy="shuffle_hash",
            )
            return Enriched(id=order.id, product_name=product.name)

    plan = _analysis(AddProduct)
    compiled_step = _body(AddProduct)
    recipe_plan = PySpark.compiler.lower()(plan)
    recipe = recipe_plan.steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})
    traceability = Compiler.traceability.build()(
        recipe_plan,
        source_transform=f"{AddProduct.__module__}.AddProduct",
        transform_module="generated.transforms.add_product",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert len(compiled_step.joins) == 1
    assert compiled_step.joins[0].method is JoinMethod.ROWSET
    assert compiled_step.joins[0].strategy is JoinStrategy.SHUFFLE_HASH
    assert compiled_step.operations[0].kind == "join"
    assert compiled_step.operations[0].join == compiled_step.joins[0]
    assert compiled_step.operations[0].capability is not None
    assert compiled_step.operations[0].capability.name == "rowset_join"
    assert compiled_step.operations[0].cardinality is OperationCardinality.ROW_MULTIPLYING
    assert '.hint("shuffle_hash").alias("products")' in text
    assert '"inner"' in text
    assert dependencies["add_product.join[1].product"].operation == "rowset_join"
    assert dependencies["add_product.join[1].product"].detail["cardinality"] == "row_multiplying"


@pytest.mark.parametrize("using", ["id", ["id"]])
def test_inner_join_accepts_using_key(using: object) -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            inner_join(on=using)
            return Enriched(id=order.id, product_name=product.name)

    recipe = _recipe(AddProduct).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert 'F.col("order.id") == F.col("products.id")' in text


def test_inner_join_accepts_multiple_using_keys() -> None:
    class CompositeOrder(Schema):
        tenant_id = string(nullable=False)
        id = string(nullable=False)

    class CompositeProduct(Schema):
        tenant_id = string(nullable=False)
        id = string(nullable=False)
        name = string(nullable=True)

    @transform
    class AddProduct(Transform):
        orders = input(CompositeOrder)
        products = input(CompositeProduct)
        enriched = output(Enriched)

        def add_product(self, order: CompositeOrder, product: CompositeProduct) -> Enriched:
            inner_join(product, on=["tenant_id", "id"])
            return Enriched(id=order.id, product_name=product.name)

    recipe = _recipe(AddProduct).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert 'F.col("composite_order.tenant_id") == F.col("products.tenant_id")' in text
    assert 'F.col("composite_order.id") == F.col("products.id")' in text


def test_right_join_explains_nullable_left_output() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            right_join(on=product.id == order.product_id)
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError) as raised:
        _compile(AddProduct)

    assert raised.value.diagnostic.code == "SCHEMA-E0301"
    assert "current left row may be absent after this right join" in raised.value.diagnostic.problem_text()


@pytest.mark.parametrize(
    ("strategy", "hint"),
    [
        (JoinStrategy.BROADCAST_HASH, "broadcast"),
        (JoinStrategy.SORT_MERGE, "merge"),
        (JoinStrategy.SHUFFLE_REPLICATE_NL, "shuffle_replicate_nl"),
    ],
)
def test_join_strategy_renders_supported_pyspark_hint(strategy: JoinStrategy, hint: str) -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            inner_join(on=product.id == order.product_id, strategy=strategy)
            return Enriched(id=order.id, product_name=product.name)

    recipe = _recipe(AddProduct).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert f'.hint("{hint}")' in text


def test_bare_right_join_records_rowset_operation() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(OuterEnriched)

        def add_product(self, order: Order, product: Product) -> OuterEnriched:
            right_join(on=product.id == order.product_id)
            return OuterEnriched(id=order.id, product_name=product.name)

    plan = _analysis(AddProduct)
    compiled_step = _body(AddProduct)
    recipe_plan = PySpark.compiler.lower()(plan)
    recipe = recipe_plan.steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})
    traceability = Compiler.traceability.build()(
        recipe_plan,
        source_transform=f"{AddProduct.__module__}.AddProduct",
        transform_module="generated.transforms.add_product",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert compiled_step.joins[0].method is JoinMethod.ROWSET
    assert compiled_step.joins[0].how is Join.RIGHT
    assert compiled_step.operations[0].capability is not None
    assert compiled_step.operations[0].capability.name == "rowset_join"
    assert compiled_step.operations[0].cardinality is OperationCardinality.ROW_MULTIPLYING
    assert '"right"' in text
    assert dependencies["add_product.join[1].product"].operation == "rowset_join"
    assert dependencies["add_product.join[1].product"].detail["cardinality"] == "row_multiplying"


def test_explicit_full_rowset_join_accepts_disjunctive_predicate() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(OuterEnriched)

        def add_product(self, order: Order, product: Product) -> OuterEnriched:
            rowset_join(
                left=order,
                right=product,
                on=(product.id == order.product_id) | (product.name == order.status),
                how=Join.FULL,
            )
            return OuterEnriched(id=order.id, product_name=product.name)

    plan = _analysis(AddProduct)
    recipe = PySpark.compiler.lower()(plan).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert _body(AddProduct).joins[0].method is JoinMethod.ROWSET
    assert _body(AddProduct).joins[0].how is Join.FULL
    assert '"full"' in text
    assert "|" in text


def test_full_join_shortcut_accepts_non_equi_predicate() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(OuterEnriched)

        def add_product(self, order: Order, product: Product) -> OuterEnriched:
            full_join(on=cast(Any, product).valid_from <= cast(Any, order).status)
            return OuterEnriched(id=order.id, product_name=product.name)

    recipe = _recipe(AddProduct).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert recipe.joins[0].method is JoinMethod.ROWSET
    assert recipe.joins[0].how is Join.FULL
    assert "<=" in text


def test_cross_join_requires_cartesian_acknowledgement() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            cross_join(product)
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(TypeError, match="allow_cartesian=True"):
        _compile(AddProduct)


def test_cross_join_renders_cross_join_call() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            cross_join(product, allow_cartesian=True)
            return Enriched(id=order.id, product_name=product.name)

    recipe = _recipe(AddProduct).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert recipe.joins[0].method is JoinMethod.ROWSET
    assert recipe.joins[0].how is Join.CROSS
    assert recipe.joins[0].assert_singleton_in_batch is False
    assert ".crossJoin(products_joined)" in text
    assert '".cross"' not in text


def test_param_join_records_a_batch_singleton_cross_join() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            param_join(product)
            return Enriched(id=order.id, product_name=product.name)

    recipe = _recipe(AddProduct).steps[0]

    assert [operation.kind for operation in recipe.operations] == ["join"]
    assert recipe.joins[0].how is Join.CROSS
    assert recipe.joins[0].assert_singleton_in_batch is True


def test_param_join_renders_runtime_batch_singleton_guard() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            param_join(product)
            return Enriched(id=order.id, product_name=product.name)

    text = PySpark.render.step()(
        _recipe(AddProduct).steps[0],
        current="orders",
        sources={"orders": "orders", "products": "products"},
    )

    assert "__structure_streaming_step = orders.isStreaming or products.isStreaming" in text
    assert "if not __structure_streaming_step:" in text
    assert "F.assert_true(F.col(\"__structure_count\") == F.lit(1)" in text
    assert text.index("if not __structure_streaming_step:") < text.index("orders = orders.crossJoin")


def test_deduped_lookup_join_records_policy_and_renders_deterministic_lookup() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            lookup_join(
                product,
                on=product.id == order.product_id,
                how="left",
                dedupe=JoinDedupe.latest_by(product.name, ties="error"),
            )
            return Enriched(id=order.id, product_name=product.name)

    plan = _analysis(AddProduct)
    compiled_step = _body(AddProduct)
    recipe = PySpark.compiler.lower()(plan).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert len(plan.diagnostics) == 0
    assert compiled_step.joins[0].dedupe is not None
    assert compiled_step.joins[0].dedupe.direction == "latest"
    assert compiled_step.joins[0].dedupe.ties is TiePolicy.ERROR
    assert recipe.joins[0].dedupe is not None
    assert (
        "F.row_number().over(Window.partitionBy(F.col(\"products.id\")).orderBy(F.col(\"products.name\").desc()))"
        in text
    )
    assert '.where(F.col("__structure_products_rank") == F.lit(1))' in text
    assert '.drop("__structure_products_rank").alias("products")' in text


def test_deduped_lookup_join_rejects_left_side_ordering() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            lookup_join(
                product,
                on=product.id == order.product_id,
                how=Join.LEFT,
                dedupe=JoinDedupe.latest_by(order.id),
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError) as raised:
        _compile(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "order_by must read only the joined input" in raised.value.diagnostic.problem_text()


def test_temporal_one_records_closed_open_validity_lookup() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            temporal_one(
                on=product.id == order.product_id,
                at=order.status,
                valid_from=product.valid_from,
                valid_to=product.valid_to,
                how=Join.LEFT,
            )
            return Enriched(id=order.id, product_name=product.name)

    plan = _analysis(AddProduct)
    compiled_step = _body(AddProduct)
    recipe_plan = PySpark.compiler.lower()(plan)
    recipe = recipe_plan.steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})
    traceability = Compiler.traceability.build()(
        recipe_plan,
        source_transform=f"{AddProduct.__module__}.AddProduct",
        transform_module="generated.transforms.add_product",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert len(compiled_step.joins) == 1
    assert compiled_step.joins[0].method is JoinMethod.TEMPORAL_ONE
    assert compiled_step.joins[0].temporal is not None
    assert compiled_step.operations[0].capability is not None
    assert compiled_step.operations[0].capability.name == "temporal_one"
    assert compiled_step.operations[0].cardinality is OperationCardinality.SELECT_ONE
    assert recipe.joins[0].temporal is not None
    assert "(F.col(\"products.valid_from\") <= F.col(\"order.status\"))" in text
    assert "((F.col(\"order.status\") < F.col(\"products.valid_to\")) | F.col(\"products.valid_to\").isNull())" in text
    assert dependencies["add_product.join[1].product"].operation == "temporal_one"
    assert dependencies["add_product.join[1].product"].detail["temporal"] == "closed_open"


def test_temporal_one_rejects_left_side_validity_bound() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            temporal_one(
                on=product.id == order.product_id,
                at=order.status,
                valid_from=order.status,
                valid_to=product.valid_to,
                how=Join.LEFT,
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError) as raised:
        _compile(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "valid_from=...) must read only the joined temporal input" in raised.value.diagnostic.problem_text()


@pytest.mark.parametrize("how", [Join.RIGHT, Join.FULL, Join.CROSS])
def test_temporal_one_rejects_non_lookup_join_modes(how: Join) -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            temporal_one(
                product,
                on=product.id == order.product_id,
                at=order.status,
                valid_from=product.valid_from,
                valid_to=product.valid_to,
                how=how,
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError) as raised:
        _compile(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert 'temporal_one(...) supports "left" and "inner"' in raised.value.diagnostic.problem_text()


def test_as_of_one_records_backward_lookup_and_renders_ranked_selection() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            as_of_one(
                on=product.id == order.product_id,
                left_time=order.status,
                right_time=product.valid_from,
                direction=AsOf.BACKWARD,
                how=Join.LEFT,
            )
            return Enriched(id=order.id, product_name=product.name)

    plan = _analysis(AddProduct)
    compiled_step = _body(AddProduct)
    recipe_plan = PySpark.compiler.lower()(plan)
    recipe = recipe_plan.steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})
    traceability = Compiler.traceability.build()(
        recipe_plan,
        source_transform=f"{AddProduct.__module__}.AddProduct",
        transform_module="generated.transforms.add_product",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert len(compiled_step.joins) == 1
    assert compiled_step.joins[0].method is JoinMethod.AS_OF_ONE
    assert compiled_step.joins[0].as_of is not None
    assert compiled_step.operations[0].capability is not None
    assert compiled_step.operations[0].capability.name == "as_of_one"
    assert compiled_step.operations[0].cardinality is OperationCardinality.SELECT_ONE
    assert recipe.joins[0].as_of is not None
    assert "F.monotonically_increasing_id()" in text
    assert "(F.col(\"products.valid_from\") <= F.col(\"order.status\"))" in text
    assert "Window.partitionBy(F.col(\"__structure_order_products_row\"))" in text
    assert ".orderBy(F.col(\"products.valid_from\").desc())" in text
    assert dependencies["add_product.join[1].product"].operation == "as_of_one"
    assert dependencies["add_product.join[1].product"].detail["as_of"] == "backward"


def test_as_of_one_records_forward_lookup_and_renders_earliest_selection() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            as_of_one(
                on=product.id == order.product_id,
                left_time=order.status,
                right_time=product.valid_from,
                direction=AsOf.FORWARD,
                how=Join.LEFT,
            )
            return Enriched(id=order.id, product_name=product.name)

    recipe = _recipe(AddProduct).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert '(F.col("products.valid_from") >= F.col("order.status"))' in text
    assert '.orderBy(F.col("products.valid_from").asc())' in text


def test_as_of_one_records_nearest_lookup_and_renders_distance_selection() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            as_of_one(
                on=product.id == order.product_id,
                left_time=order.status,
                right_time=product.valid_from,
                direction="nearest",
                how=Join.LEFT,
            )
            return Enriched(id=order.id, product_name=product.name)

    recipe_plan = _recipe(AddProduct)
    recipe = recipe_plan.steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})
    traceability = Compiler.traceability.build()(
        recipe_plan,
        source_transform=f"{AddProduct.__module__}.AddProduct",
        transform_module="generated.transforms.add_product",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert recipe.joins[0].as_of is not None
    assert recipe.joins[0].as_of.direction is AsOf.NEAREST
    assert 'F.abs(F.col("products.valid_from") - F.col("order.status"))' in text
    assert "__structure_products_as_of_min_distance" in text
    assert "__structure_products_as_of_tie_count" in text
    assert "as_of_one(direction='nearest', ties='error') found equidistant matches" in text
    assert 'orderBy(F.col("__structure_products_as_of_distance").asc(), F.col("products.valid_from").asc())' in text
    assert dependencies["add_product.join[1].product"].operation == "as_of_one"
    assert dependencies["add_product.join[1].product"].detail["as_of"] == "nearest"


def test_as_of_one_rejects_left_side_right_time() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            as_of_one(
                on=product.id == order.product_id,
                left_time=order.status,
                right_time=order.status,
                how=Join.LEFT,
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError) as raised:
        _compile(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "right_time=...) must read only the joined as-of input" in raised.value.diagnostic.problem_text()


@pytest.mark.parametrize("how", [Join.RIGHT, Join.FULL, Join.CROSS])
def test_as_of_one_rejects_non_lookup_join_modes(how: Join) -> None:
    @transform
    class AddProduct(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            as_of_one(
                product,
                on=product.id == order.product_id,
                left_time=order.status,
                right_time=product.valid_from,
                how=how,
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError) as raised:
        _compile(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert 'as_of_one(...) supports "left" and "inner"' in raised.value.diagnostic.problem_text()


def test_exists_join_does_not_make_relation_fields_readable() -> None:
    @transform
    class PublishKnownProducts(Transform):
        orders = input(Order)
        products = input(Product)
        enriched = output(Enriched)

        def publish(self, order: Order, product: Product) -> Enriched:
            where(exists(on=product.id == order.product_id))
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError) as raised:
        _compile(PublishKnownProducts)

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
            lookup_join(product, on=product.id == order.product_id, how=Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(StructureCompileError) as raised:
        _compile(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "reads relation parameter product before it is joined" in raised.value.diagnostic.problem_text()


def test_source_less_project_uses_driving_row() -> None:
    @transform
    class Publish(Transform):
        orders = input(Order)
        published = output(Published)

        def publish(self, order: Order) -> Published:
            return project(Published)

    assert [assignment.field.name for assignment in _body(Publish).projection] == ["id", "status"]


def test_return_chain_join_where_project_uses_ordered_operations() -> None:
    @transform
    class Publish(Transform):
        orders = input(Order)
        products = input(Product)
        published = output(Published)

        def publish(self, order: Order, product: Product) -> Published:
            return (
                cast(Any, lookup_join(product, on=product.id == order.product_id))
                .where(cast(Any, order).status.is_not_null())
                .project(Published)
            )

    compiled_step = _body(Publish)

    assert [operation.kind for operation in compiled_step.operations] == ["join", "filter"]
    assert [assignment.field.name for assignment in compiled_step.projection] == ["id", "status"]


def test_method_cache_option_records_optimization_operation() -> None:
    @transform
    class Publish(Transform):
        orders = input(Order)
        published = output(Published)

        @step(cache=True)
        def publish(self, order: Order) -> Published:
            return Published(id=order.id, status=order.status)

    compiled_step = _body(Publish)

    assert [operation.kind for operation in compiled_step.operations] == ["cache"]
    assert compiled_step.operations[0].capability is not None
    assert compiled_step.operations[0].capability.group == "optimization"
    assert compiled_step.operations[0].capability.name == "cache"
    assert compiled_step.operations[0].cardinality is OperationCardinality.ROW_PRESERVING
    assert compiled_step.operations[0].streaming is StreamingSupport.BATCH_ONLY
