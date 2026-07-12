from typing import Any, cast

import pytest

import structure
from structure import step as dsl_step
from structure import temporal_one, transform, where
from structure.app.compiler.api import Compiler, OperationCardinality, StreamingSupport
from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import PySpark


class Order(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    product_id = structure.field(structure.String(), nullable=False)
    status = structure.field(structure.String(), nullable=True)


class Product(structure.Structure):
    id = structure.field(structure.String(), nullable=False, primary_key=True)
    name = structure.field(structure.String(), nullable=False)
    valid_from = structure.field(structure.String(), nullable=False)
    valid_to = structure.field(structure.String(), nullable=True)


class Published(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    status = structure.field(structure.String(), nullable=True)


class Enriched(structure.Structure):
    id = structure.field(structure.String(), nullable=False)
    product_name = structure.field(structure.String(), nullable=True)


def test_where_before_join_renders_before_join() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            where(cast(Any, order.status).is_not_null())
            structure.lookup_join(product, on=product.id == order.product_id, how=structure.Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    step = PySpark.plan.lower()(compile_transform(AddProduct)).steps[0]
    text = PySpark.render.step()(step, current="orders", sources={"products": "products"})

    assert text.index("orders = orders.where(") < text.index("orders = orders.join(")


def test_where_after_join_renders_after_join() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.lookup_join(product, on=product.id == order.product_id, how=structure.Join.LEFT)
            where(cast(Any, product).name.is_not_null())
            return Enriched(id=order.id, product_name=product.name)

    step = PySpark.plan.lower()(compile_transform(AddProduct)).steps[0]
    text = PySpark.render.step()(step, current="orders", sources={"products": "products"})

    assert text.index("orders = orders.join(") < text.index("orders = orders.where(")


def test_bare_lookup_join_makes_later_relation_reads_joined() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.lookup_join(product, on=product.id == order.product_id, how=structure.Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    plan = compile_transform(AddProduct)
    projection = {assignment.field.name: assignment.expression for assignment in plan.steps[0].projection}
    product_name = projection["product_name"]
    product_name_data = cast(dict[str, object], product_name.data)

    assert plan.steps[0].joins[0].source == "products"
    assert product_name_data["scope"] == "product"
    assert product_name.nullable


def test_bare_inferred_lookup_join_makes_later_relation_reads_joined() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.lookup_join(on=product.id == order.product_id, how=structure.Join.LEFT)
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
    assert step.operations[0].capability.name == "lookup_join"
    assert step.operations[0].cardinality is OperationCardinality.SELECT_ONE
    assert product_name_data["scope"] == "product"
    assert product_name.nullable


def test_inferred_lookup_join_preserves_filter_join_order() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            where(cast(Any, order.status).is_not_null())
            structure.lookup_join(on=product.id == order.product_id, how=structure.Join.LEFT)
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
    class PublishKnownProducts(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        published = structure.output(Published)

        def publish(self, order: Order, product: Product) -> Published:
            where(structure.exists(on=product.id == order.product_id))
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
    class PublishUnknownProducts(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        published = structure.output(Published)

        def publish(self, order: Order, product: Product) -> Published:
            where(structure.not_exists(on=product.id == order.product_id))
            return Published(id=order.id, status=order.status)

    step = compile_transform(PublishUnknownProducts).steps[0]
    recipe = PySpark.plan.lower()(compile_transform(PublishUnknownProducts)).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert step.joins[0].method is JoinMethod.NOT_EXISTS
    assert step.operations[0].capability is not None
    assert step.operations[0].capability.name == "not_exists"
    assert step.operations[0].cardinality is OperationCardinality.ROW_FILTERING
    assert '"left_anti"' in text


def test_inner_join_records_row_multiplying_operation() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.inner_join(
                on=product.id == order.product_id,
                strategy=structure.JoinStrategy.SHUFFLE_HASH,
            )
            return Enriched(id=order.id, product_name=product.name)

    plan = compile_transform(AddProduct)
    step = plan.steps[0]
    recipe_plan = PySpark.plan.lower()(plan)
    recipe = recipe_plan.steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})
    traceability = Compiler.traceability.build()(
        recipe_plan,
        source_transform=f"{AddProduct.__module__}.AddProduct",
        transform_module="generated.transforms.add_product",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert len(step.joins) == 1
    assert step.joins[0].method is JoinMethod.ROWSET
    assert step.joins[0].strategy is structure.JoinStrategy.SHUFFLE_HASH
    assert step.operations[0].kind == "join"
    assert step.operations[0].join == step.joins[0]
    assert step.operations[0].capability is not None
    assert step.operations[0].capability.name == "rowset_join"
    assert step.operations[0].cardinality is OperationCardinality.ROW_MULTIPLYING
    assert '.hint("shuffle_hash").alias("products")' in text
    assert '"inner"' in text
    assert dependencies["add_product.join[1].product"].operation == "rowset_join"
    assert dependencies["add_product.join[1].product"].detail["cardinality"] == "row_multiplying"


def test_bare_right_join_records_rowset_operation() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.right_join(on=product.id == order.product_id)
            return Enriched(id=order.id, product_name=product.name)

    plan = compile_transform(AddProduct)
    step = plan.steps[0]
    recipe_plan = PySpark.plan.lower()(plan)
    recipe = recipe_plan.steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})
    traceability = Compiler.traceability.build()(
        recipe_plan,
        source_transform=f"{AddProduct.__module__}.AddProduct",
        transform_module="generated.transforms.add_product",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert step.joins[0].method is JoinMethod.ROWSET
    assert step.joins[0].how is structure.Join.RIGHT
    assert step.operations[0].capability is not None
    assert step.operations[0].capability.name == "rowset_join"
    assert step.operations[0].cardinality is OperationCardinality.ROW_MULTIPLYING
    assert '"right"' in text
    assert dependencies["add_product.join[1].product"].operation == "rowset_join"
    assert dependencies["add_product.join[1].product"].detail["cardinality"] == "row_multiplying"


def test_explicit_full_rowset_join_accepts_disjunctive_predicate() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.rowset_join(
                left=order,
                right=product,
                on=(product.id == order.product_id) | (product.name == order.status),
                how=structure.Join.FULL,
            )
            return Enriched(id=order.id, product_name=product.name)

    plan = compile_transform(AddProduct)
    recipe = PySpark.plan.lower()(plan).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert plan.steps[0].joins[0].method is JoinMethod.ROWSET
    assert plan.steps[0].joins[0].how is structure.Join.FULL
    assert '"full"' in text
    assert "|" in text


def test_full_join_shortcut_accepts_non_equi_predicate() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.full_join(on=cast(Any, product).valid_from <= cast(Any, order).status)
            return Enriched(id=order.id, product_name=product.name)

    recipe = PySpark.plan.lower()(compile_transform(AddProduct)).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert recipe.joins[0].method is JoinMethod.ROWSET
    assert recipe.joins[0].how is structure.Join.FULL
    assert "<=" in text


def test_cross_join_requires_cartesian_acknowledgement() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.cross_join(product)
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(TypeError, match="allow_cartesian=True"):
        compile_transform(AddProduct)


def test_cross_join_renders_cross_join_call() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.cross_join(product, allow_cartesian=True)
            return Enriched(id=order.id, product_name=product.name)

    recipe = PySpark.plan.lower()(compile_transform(AddProduct)).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert recipe.joins[0].method is JoinMethod.ROWSET
    assert recipe.joins[0].how is structure.Join.CROSS
    assert ".crossJoin(products_joined)" in text
    assert '".cross"' not in text


def test_deduped_lookup_join_records_policy_and_renders_deterministic_lookup() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.lookup_join(
                product,
                on=product.id == order.product_id,
                how=structure.Join.LEFT,
                dedupe=structure.JoinDedupe.latest_by(product.name),
            )
            return Enriched(id=order.id, product_name=product.name)

    plan = compile_transform(AddProduct)
    step = plan.steps[0]
    recipe = PySpark.plan.lower()(plan).steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})

    assert len(plan.diagnostics) == 0
    assert step.joins[0].dedupe is not None
    assert step.joins[0].dedupe.direction == "latest"
    assert step.joins[0].dedupe.ties is structure.TiePolicy.ERROR
    assert recipe.joins[0].dedupe is not None
    assert (
        "F.row_number().over(Window.partitionBy(F.col(\"products.id\")).orderBy(F.col(\"products.name\").desc()))"
        in text
    )
    assert '.where(F.col("__structure_products_rank") == F.lit(1))' in text
    assert '.drop("__structure_products_rank").alias("products")' in text


def test_deduped_lookup_join_rejects_left_side_ordering() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.lookup_join(
                product,
                on=product.id == order.product_id,
                how=structure.Join.LEFT,
                dedupe=structure.JoinDedupe.latest_by(order.id),
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "order_by must read only the joined input" in raised.value.diagnostic.problem_text()


def test_temporal_one_records_closed_open_validity_lookup() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            temporal_one(
                on=product.id == order.product_id,
                at=order.status,
                valid_from=product.valid_from,
                valid_to=product.valid_to,
                how=structure.Join.LEFT,
            )
            return Enriched(id=order.id, product_name=product.name)

    plan = compile_transform(AddProduct)
    step = plan.steps[0]
    recipe_plan = PySpark.plan.lower()(plan)
    recipe = recipe_plan.steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})
    traceability = Compiler.traceability.build()(
        recipe_plan,
        source_transform=f"{AddProduct.__module__}.AddProduct",
        transform_module="generated.transforms.add_product",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert len(step.joins) == 1
    assert step.joins[0].method is JoinMethod.TEMPORAL_ONE
    assert step.joins[0].temporal is not None
    assert step.operations[0].capability is not None
    assert step.operations[0].capability.name == "temporal_one"
    assert step.operations[0].cardinality is OperationCardinality.SELECT_ONE
    assert recipe.joins[0].temporal is not None
    assert "(F.col(\"products.valid_from\") <= F.col(\"order.status\"))" in text
    assert "((F.col(\"order.status\") < F.col(\"products.valid_to\")) | F.col(\"products.valid_to\").isNull())" in text
    assert dependencies["add_product.join[1].product"].operation == "temporal_one"
    assert dependencies["add_product.join[1].product"].detail["temporal"] == "closed_open"


def test_temporal_one_rejects_left_side_validity_bound() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            temporal_one(
                on=product.id == order.product_id,
                at=order.status,
                valid_from=order.status,
                valid_to=product.valid_to,
                how=structure.Join.LEFT,
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "valid_from=...) must read only the joined temporal input" in raised.value.diagnostic.problem_text()


def test_as_of_one_records_backward_lookup_and_renders_ranked_selection() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.as_of_one(
                on=product.id == order.product_id,
                left_time=order.status,
                right_time=product.valid_from,
                direction=structure.AsOf.BACKWARD,
                how=structure.Join.LEFT,
            )
            return Enriched(id=order.id, product_name=product.name)

    plan = compile_transform(AddProduct)
    step = plan.steps[0]
    recipe_plan = PySpark.plan.lower()(plan)
    recipe = recipe_plan.steps[0]
    text = PySpark.render.step()(recipe, current="orders", sources={"products": "products"})
    traceability = Compiler.traceability.build()(
        recipe_plan,
        source_transform=f"{AddProduct.__module__}.AddProduct",
        transform_module="generated.transforms.add_product",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert len(step.joins) == 1
    assert step.joins[0].method is JoinMethod.AS_OF_ONE
    assert step.joins[0].as_of is not None
    assert step.operations[0].capability is not None
    assert step.operations[0].capability.name == "as_of_one"
    assert step.operations[0].cardinality is OperationCardinality.SELECT_ONE
    assert recipe.joins[0].as_of is not None
    assert "F.monotonically_increasing_id()" in text
    assert "(F.col(\"products.valid_from\") <= F.col(\"order.status\"))" in text
    assert "Window.partitionBy(F.col(\"__structure_order_products_row\"))" in text
    assert ".orderBy(F.col(\"products.valid_from\").desc())" in text
    assert dependencies["add_product.join[1].product"].operation == "as_of_one"
    assert dependencies["add_product.join[1].product"].detail["as_of"] == "backward"


def test_as_of_one_rejects_left_side_right_time() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            structure.as_of_one(
                on=product.id == order.product_id,
                left_time=order.status,
                right_time=order.status,
                how=structure.Join.LEFT,
            )
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "right_time=...) must read only the joined as-of input" in raised.value.diagnostic.problem_text()


def test_exists_join_does_not_make_relation_fields_readable() -> None:
    @transform
    class PublishKnownProducts(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def publish(self, order: Order, product: Product) -> Enriched:
            where(structure.exists(on=product.id == order.product_id))
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(PublishKnownProducts)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "reads relation parameter product before it is joined" in raised.value.diagnostic.problem_text()


def test_pre_join_relation_filter_still_fails() -> None:
    @transform
    class AddProduct(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        enriched = structure.output(Enriched)

        def add_product(self, order: Order, product: Product) -> Enriched:
            where(cast(Any, product).name.is_not_null())
            structure.lookup_join(product, on=product.id == order.product_id, how=structure.Join.LEFT)
            return Enriched(id=order.id, product_name=product.name)

    with pytest.raises(structure.StructureCompileError) as raised:
        compile_transform(AddProduct)

    assert raised.value.diagnostic.code == "JOIN-E0601"
    assert "reads relation parameter product before it is joined" in raised.value.diagnostic.problem_text()


def test_source_less_project_uses_driving_row() -> None:
    @transform
    class Publish(structure.Transform):
        orders = structure.input(Order)
        published = structure.output(Published)

        def publish(self, order: Order) -> Published:
            return structure.project(Published)

    plan = compile_transform(Publish)

    assert [assignment.field.name for assignment in plan.steps[0].projection] == ["id", "status"]


def test_return_chain_join_where_project_uses_ordered_operations() -> None:
    @transform
    class Publish(structure.Transform):
        orders = structure.input(Order)
        products = structure.input(Product)
        published = structure.output(Published)

        def publish(self, order: Order, product: Product) -> Published:
            return (
                cast(Any, structure.lookup_join(product, on=product.id == order.product_id))
                .where(cast(Any, order).status.is_not_null())
                .project(Published)
            )

    step = compile_transform(Publish).steps[0]

    assert [operation.kind for operation in step.operations] == ["join", "filter"]
    assert [assignment.field.name for assignment in step.projection] == ["id", "status"]


def test_method_cache_option_records_optimization_operation() -> None:
    @transform
    class Publish(structure.Transform):
        orders = structure.input(Order)
        published = structure.output(Published)

        @dsl_step(cache=True)
        def publish(self, order: Order) -> Published:
            return Published(id=order.id, status=order.status)

    step = compile_transform(Publish).steps[0]

    assert [operation.kind for operation in step.operations] == ["cache"]
    assert step.operations[0].capability is not None
    assert step.operations[0].capability.group == "optimization"
    assert step.operations[0].capability.name == "cache"
    assert step.operations[0].cardinality is OperationCardinality.ROW_PRESERVING
    assert step.operations[0].streaming is StreamingSupport.BATCH_ONLY
