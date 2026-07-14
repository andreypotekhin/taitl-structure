import pytest

from structure import *
from structure.app.compiler.api import Compiler
from structure.app.target.pyspark.api import PySpark


class OrderRaw(Schema):
    id = field.string(nullable=False)
    product_id = field.string(nullable=False)


class Product(Schema):
    id = field.string(nullable=False)
    name = field.string(nullable=False)


class OrderWithProduct(Schema):
    id = field.string(nullable=False)
    product_name = field.string(nullable=True)


def test_multiple_schema_parameters_and_results_compile_in_order() -> None:
    @transform
    class AddProduct(Transform):
        orders_external = input(OrderRaw)
        orders_internal = input(OrderRaw)
        products = input(Product)
        accepted = output(OrderWithProduct)
        audited = output(OrderWithProduct)

        @step(
            input=[orders_external, products],
            output=[accepted, audited],
        )
        def add_product(
            self,
            order: OrderRaw,
            product: Product,
        ) -> tuple[OrderWithProduct, OrderWithProduct]:
            product = lookup_join(
                product,
                on=product.id == order.product_id,
                how=Join.LEFT,
            )
            accepted_order = OrderWithProduct(id=order.id, product_name=product.name)
            audited_order = OrderWithProduct(id=order.id, product_name=product.name)
            return accepted_order, audited_order

    plan = compile_transform(AddProduct)
    compiled_step = plan.steps[0]

    assert [(item.parameter, item.source, item.driving) for item in compiled_step.inputs] == [
        ("order", "orders_external", True),
        ("product", "products", False),
    ]
    assert [(item.lane, item.frame) for item in compiled_step.results] == [
        ("accepted", "accepted"),
        ("audited", "audited"),
    ]
    assert compiled_step.joins[0].source == "products"
    assert [item.source for item in plan.outputs] == ["accepted", "audited"]


def test_unique_schema_parameters_are_inferred() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        enriched = output(OrderWithProduct)

        def add_product(self, order: OrderRaw, product: Product) -> OrderWithProduct:
            product = lookup_join(product, on=product.id == order.product_id)
            return OrderWithProduct(id=order.id, product_name=product.name)

    compiled_step = compile_transform(AddProduct).steps[0]

    assert [item.source for item in compiled_step.inputs] == ["orders", "products"]


def test_plural_parameter_name_disambiguates_same_schema_driving_inputs() -> None:
    @transform
    class PickOrders(Transform):
        orders1 = input(OrderRaw)
        orders2 = input(OrderRaw)
        picked = output(OrderRaw)

        @step(output=picked)
        def pick(self, order1: OrderRaw) -> OrderRaw:
            return OrderRaw(id=order1.id, product_id=order1.product_id)

    compiled_step = compile_transform(PickOrders).steps[0]

    assert [(item.parameter, item.source, item.lane) for item in compiled_step.inputs] == [
        ("order1", "orders1", "orders1"),
    ]


def test_plural_parameter_name_disambiguates_same_schema_relation_inputs() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products1 = input(Product)
        products2 = input(Product)
        enriched = output(OrderWithProduct)

        @step(output=enriched)
        def add_product(self, order: OrderRaw, product2: Product) -> OrderWithProduct:
            product2 = lookup_join(product2, on=product2.id == order.product_id)
            return OrderWithProduct(id=order.id, product_name=product2.name)

    compiled_step = compile_transform(AddProduct).steps[0]

    assert [(item.parameter, item.source, item.lane) for item in compiled_step.inputs] == [
        ("order", "orders", "orders"),
        ("product2", "products2", "products2"),
    ]


def test_plural_parameter_name_disambiguates_same_schema_lanes() -> None:
    @transform
    class PickOrders(Transform):
        raw1 = input(OrderRaw)
        raw2 = input(OrderRaw)
        orders1 = lane(OrderRaw)
        orders2 = lane(OrderRaw)
        picked = output(OrderRaw)

        @step(input=raw1, output=orders1)
        def seed1(self, order: OrderRaw) -> OrderRaw:
            return OrderRaw(id=order.id, product_id=order.product_id)

        @step(input=raw2, output=orders2)
        def seed2(self, order: OrderRaw) -> OrderRaw:
            return OrderRaw(id=order.id, product_id=order.product_id)

        @step(output=picked)
        def pick(self, order2: OrderRaw) -> OrderRaw:
            return OrderRaw(id=order2.id, product_id=order2.product_id)

    compiled_step = compile_transform(PickOrders).steps[2]

    assert [(item.parameter, item.source, item.lane) for item in compiled_step.inputs] == [
        ("order2", "orders2", "orders2"),
    ]


def test_partial_explicit_input_declaration_disables_plural_parameter_inference() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products1 = input(Product)
        products2 = input(Product)
        enriched = output(OrderWithProduct)

        @step(input=orders, output=enriched)
        def add_product(self, order: OrderRaw, product2: Product) -> OrderWithProduct:
            product2 = lookup_join(product2, on=product2.id == order.product_id)
            return OrderWithProduct(id=order.id, product_name=product2.name)

    with pytest.raises(Exception, match="@transform\\(input=\\.\\.\\.\\) binds 1 source"):
        compile_transform(AddProduct)


def test_join_relation_can_be_inferred_from_on_clause() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        enriched = output(OrderWithProduct)

        def add_product(self, order: OrderRaw, product: Product) -> OrderWithProduct:
            lookup_join(on=product.id == order.product_id, how=Join.LEFT)
            return OrderWithProduct(id=order.id, product_name=product.name)

    compiled_step = compile_transform(AddProduct).steps[0]

    assert compiled_step.joins[0].input_name == "product"
    assert compiled_step.joins[0].source == "products"
    assert compiled_step.operations[0].kind == "join"
    assert compiled_step.operations[0].join == compiled_step.joins[0]


def test_join_relation_can_be_inferred_from_reversed_operands() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        enriched = output(OrderWithProduct)

        def add_product(self, order: OrderRaw, product: Product) -> OrderWithProduct:
            lookup_join(on=order.product_id == product.id, how=Join.LEFT)
            return OrderWithProduct(id=order.id, product_name=product.name)

    compiled_step = compile_transform(AddProduct).steps[0]

    assert compiled_step.joins[0].input_name == "product"
    assert compiled_step.joins[0].source == "products"


def test_join_relation_can_be_inferred_from_class_input_scope() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        enriched = output(OrderWithProduct)

        def add_product(self, order: OrderRaw) -> OrderWithProduct:
            lookup_join(on=self.products.id == order.product_id, how=Join.LEFT)
            return OrderWithProduct(id=order.id, product_name=self.products.name)

    compiled_step = compile_transform(AddProduct).steps[0]
    projection = {assignment.field.name: assignment.expression for assignment in compiled_step.projection}

    assert compiled_step.joins[0].input_name == "products"
    assert compiled_step.joins[0].source == "products"
    assert projection["product_name"].nullable


class ProductAlias(Schema):
    id = field.string(nullable=False)
    name = field.string(nullable=False)


class OrderWithProductAlias(Schema):
    id = field.string(nullable=False)
    product_name = field.string(nullable=True)
    alias_name = field.string(nullable=True)


def test_serial_join_relation_can_be_inferred_from_earlier_joined_scope() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        aliases = input(ProductAlias)
        enriched = output(OrderWithProductAlias)

        def add_product(
            self,
            order: OrderRaw,
            product: Product,
            alias: ProductAlias,
        ) -> OrderWithProductAlias:
            lookup_join(on=product.id == order.product_id, how=Join.LEFT)
            lookup_join(on=alias.id == product.id, how=Join.LEFT)
            return OrderWithProductAlias(
                id=order.id,
                product_name=product.name,
                alias_name=alias.name,
            )

    compiled_step = compile_transform(AddProduct).steps[0]

    assert [join.input_name for join in compiled_step.joins] == ["product", "alias"]
    assert [operation.kind for operation in compiled_step.operations] == ["join", "join"]


def test_array_input_binds_lane_parameters_in_order() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        order_lane = lane(OrderRaw)
        product_lane = lane(Product)
        enriched = output(OrderWithProduct)

        @step(input=orders, output=order_lane)
        def seed_order(self, order: OrderRaw) -> OrderRaw:
            return OrderRaw(id=order.id, product_id=order.product_id)

        @step(input=products, output=product_lane)
        def seed_product(self, product: Product) -> Product:
            return Product(id=product.id, name=product.name)

        @step(input=[order_lane, product_lane], output=enriched)
        def add_product(self, order: OrderRaw, product: Product) -> OrderWithProduct:
            product = lookup_join(product, on=product.id == order.product_id)
            return OrderWithProduct(id=order.id, product_name=product.name)

    compiled_step = compile_transform(AddProduct).steps[2]

    assert [(item.parameter, item.source, item.lane) for item in compiled_step.inputs] == [
        ("order", "order_lane", "order_lane"),
        ("product", "product_lane", "product_lane"),
    ]
    assert compiled_step.output_lane == "enriched"


def test_repeated_schema_parameters_require_explicit_inputs() -> None:
    @transform
    class AddProduct(Transform):
        orders_external = input(OrderRaw)
        orders_internal = input(OrderRaw)
        products = input(Product)
        enriched = output(OrderWithProduct)

        def add_product(self, order: OrderRaw, product: Product) -> OrderWithProduct:
            product = lookup_join(product, on=product.id == order.product_id)
            return OrderWithProduct(id=order.id, product_name=product.name)

    try:
        compile_transform(AddProduct)
    except Exception as error:
        message = str(error)
    else:
        raise AssertionError("Expected ambiguous input diagnostic")

    assert "matched sources: orders_external, orders_internal" in message
    assert "@transform(input=[...])" in message


def test_multi_result_after_hooks_select_their_dataframe() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        accepted = output(OrderWithProduct)
        audited = output(OrderWithProduct)

        @step(input=[orders, products], output=[accepted, audited])
        def add_product(
            self,
            order: OrderRaw,
            product: Product,
        ) -> tuple[OrderWithProduct, OrderWithProduct]:
            product = lookup_join(product, on=product.id == order.product_id)
            row = OrderWithProduct(id=order.id, product_name=product.name)
            return row, row

        @raw(inout=lane(audited) | output(audited))
        def audit(self, *, audited, spark, ctx):
            return audited

    compiled_step = compile_transform(AddProduct).steps[0]

    assert not compiled_step.results[0].after_hooks
    assert [hook.name for hook in compiled_step.results[1].after_hooks] == ["audit"]


def test_generated_multi_result_step_uses_output_names_as_frames() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        accepted = output(OrderWithProduct)
        audited = output(OrderWithProduct)

        @step(input=[orders, products], output=[accepted, audited])
        def add_product(
            self,
            order: OrderRaw,
            product: Product,
        ) -> tuple[OrderWithProduct, OrderWithProduct]:
            product = lookup_join(product, on=product.id == order.product_id)
            row = OrderWithProduct(id=order.id, product_name=product.name)
            return row, row

        @raw(inout=lane(audited) | output(audited))
        def audit(self, *, audited, spark, ctx):
            return audited

    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(AddProduct)),
        source_transform="tests.specifications.multiple_schema_parameters.AddProduct",
        runtime_module="testing.runtime",
        schema_modules={
            OrderRaw: "testing.schemas",
            Product: "testing.schemas",
            OrderWithProduct: "testing.schemas",
        },
    )

    assert text.count(" = add_product_base.join(") == 1
    assert "        accepted = add_product_base.select(" in text
    assert "        audited = add_product_base.select(" in text
    assert "        audited = self._impl.audit(audited=audited, spark=self.spark, ctx=self.ctx)" in text
    assert (
        'return TransformResult({"accepted": accepted, "audited": audited}, single=False, '
        'schema={"accepted": ORDER_WITH_PRODUCT_SCHEMA, "audited": ORDER_WITH_PRODUCT_SCHEMA})' in text
    )

    traceability = Compiler.traceability.build()(
        PySpark.plan.lower()(compile_transform(AddProduct)),
        source_transform="tests.specifications.multiple_schema_parameters.AddProduct",
        transform_module="testing.generated.AddProductGenerated",
    )
    projections = [dependency for dependency in traceability.static_dataflow if dependency.operation == "project"]
    assert {dependency.detail.get("result") for dependency in projections} == {"accepted", "audited"}
    assert len([dependency for dependency in traceability.static_dataflow if dependency.operation == "step"]) == 1


def test_generated_plural_lane_hook_replaces_outputs_in_order() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        accepted = output(OrderWithProduct)
        audited = output(OrderWithProduct)

        @step(input=[orders, products], output=[accepted, audited])
        def add_product(
            self,
            order: OrderRaw,
            product: Product,
        ) -> tuple[OrderWithProduct, OrderWithProduct]:
            product = lookup_join(product, on=product.id == order.product_id)
            row = OrderWithProduct(id=order.id, product_name=product.name)
            return row, row

        @raw(inout=lane(accepted) | [output(accepted), output(audited)])
        def polish(self, *, accepted, audited, spark, ctx):
            return accepted, audited

    text = PySpark.render.transform()(
        PySpark.plan.lower()(compile_transform(AddProduct)),
        source_transform="tests.specifications.multiple_schema_parameters.AddProduct",
        runtime_module="testing.runtime",
        schema_modules={
            OrderRaw: "testing.schemas",
            Product: "testing.schemas",
            OrderWithProduct: "testing.schemas",
        },
    )

    assert (
        "        accepted, audited = self._impl.polish("
        "accepted=accepted, audited=audited, spark=self.spark, ctx=self.ctx)"
    ) in text


def test_multi_result_raw_hook_rejects_unproduced_output_selection() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        accepted = output(OrderWithProduct)
        audited = output(OrderWithProduct)

        @step(input=[orders, products], output=[accepted, audited])
        def add_product(
            self,
            order: OrderRaw,
            product: Product,
        ) -> tuple[OrderWithProduct, OrderWithProduct]:
            product = lookup_join(product, on=product.id == order.product_id)
            row = OrderWithProduct(id=order.id, product_name=product.name)
            return row, row

        @raw(inout=lane(orders) | lane(orders))
        def audit(self, *, orders, spark, ctx):
            return orders

    with pytest.raises(Exception, match="not available"):
        compile_transform(AddProduct)


def test_hook_signature_must_match_selected_lane() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        enriched = output(OrderWithProduct)

        @step(output=enriched)
        def add_product(self, order: OrderRaw) -> OrderWithProduct:
            return OrderWithProduct(id=order.id, product_name=None)

        @raw(inout=lane(enriched) | output(enriched))
        def audit(self, *, df, spark, ctx):
            return df

    with pytest.raises(Exception, match="enriched, spark, ctx"):
        compile_transform(AddProduct)


def test_relation_parameter_must_be_joined_before_projection() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        enriched = output(OrderWithProduct)

        def add_product(self, order: OrderRaw, product: Product) -> OrderWithProduct:
            return OrderWithProduct(id=order.id, product_name=product.name)

    with pytest.raises(Exception, match="reads relation parameter product before it is joined"):
        compile_transform(AddProduct)


def test_multiple_results_require_fixed_schema_tuple_annotation() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        enriched = output(OrderWithProduct)

        def add_product(self, order: OrderRaw) -> tuple[OrderWithProduct, ...]:
            return (OrderWithProduct(id=order.id, product_name=None),)

    with pytest.raises(Exception, match="invalid tuple return annotation"):
        compile_transform(AddProduct)
