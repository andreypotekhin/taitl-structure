import pytest

from structure import Join, String, Structure, Transform, after, field, input, join_one, output, transform
from structure.app.compiler.traceability.api import build_compiler_traceability
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import pyspark


class OrderRaw(Structure):
    id = field(String(), nullable=False)
    product_id = field(String(), nullable=False)


class Product(Structure):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=False)


class OrderWithProduct(Structure):
    id = field(String(), nullable=False)
    product_name = field(String(), nullable=True)


def test_multiple_schema_parameters_and_results_compile_in_order() -> None:
    @transform
    class AddProduct(Transform):
        orders_external = input(OrderRaw)
        orders_internal = input(OrderRaw)
        products = input(Product)
        accepted = output(OrderWithProduct)
        audited = output(OrderWithProduct)

        @transform(
            inputs=[orders_external, products],
            outputs=[accepted, audited],
        )
        def add_product(
            self,
            order: OrderRaw,
            product: Product,
        ) -> tuple[OrderWithProduct, OrderWithProduct]:
            product = join_one(
                product,
                on=product.id == order.product_id,
                how=Join.LEFT,
            )
            accepted_order = OrderWithProduct(id=order.id, product_name=product.name)
            audited_order = OrderWithProduct(id=order.id, product_name=product.name)
            return accepted_order, audited_order

    plan = compile_transform(AddProduct)
    step = plan.steps[0]

    assert [(item.parameter, item.source, item.driving) for item in step.inputs] == [
        ("order", "orders_external", True),
        ("product", "products", False),
    ]
    assert [(item.lane, item.frame) for item in step.results] == [
        ("accepted", "accepted"),
        ("audited", "audited"),
    ]
    assert step.joins[0].source == "products"
    assert [item.source for item in plan.outputs] == ["accepted", "audited"]


def test_unique_schema_parameters_are_inferred() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        enriched = output(OrderWithProduct)

        def add_product(self, order: OrderRaw, product: Product) -> OrderWithProduct:
            product = join_one(product, on=product.id == order.product_id)
            return OrderWithProduct(id=order.id, product_name=product.name)

    step = compile_transform(AddProduct).steps[0]

    assert [item.source for item in step.inputs] == ["orders", "products"]


def test_repeated_schema_parameters_require_explicit_inputs() -> None:
    @transform
    class AddProduct(Transform):
        orders_external = input(OrderRaw)
        orders_internal = input(OrderRaw)
        products = input(Product)
        enriched = output(OrderWithProduct)

        def add_product(self, order: OrderRaw, product: Product) -> OrderWithProduct:
            product = join_one(product, on=product.id == order.product_id)
            return OrderWithProduct(id=order.id, product_name=product.name)

    try:
        compile_transform(AddProduct)
    except Exception as error:
        message = str(error)
    else:
        raise AssertionError("Expected ambiguous input diagnostic")

    assert "matched sources: orders_external, orders_internal" in message
    assert "@transform(inputs=[...])" in message


def test_multi_result_after_hooks_select_their_dataframe() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        accepted = output(OrderWithProduct)
        audited = output(OrderWithProduct)

        @transform(inputs=[orders, products], outputs=[accepted, audited])
        def add_product(
            self,
            order: OrderRaw,
            product: Product,
        ) -> tuple[OrderWithProduct, OrderWithProduct]:
            product = join_one(product, on=product.id == order.product_id)
            row = OrderWithProduct(id=order.id, product_name=product.name)
            return row, row

        @after(add_product, df=audited)
        def audit(self, *, df, spark, ctx):
            return df

    step = compile_transform(AddProduct).steps[0]

    assert not step.results[0].after_hooks
    assert [hook.name for hook in step.results[1].after_hooks] == ["audit"]


def test_generated_multi_result_step_uses_output_names_as_frames() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        accepted = output(OrderWithProduct)
        audited = output(OrderWithProduct)

        @transform(inputs=[orders, products], outputs=[accepted, audited])
        def add_product(
            self,
            order: OrderRaw,
            product: Product,
        ) -> tuple[OrderWithProduct, OrderWithProduct]:
            product = join_one(product, on=product.id == order.product_id)
            row = OrderWithProduct(id=order.id, product_name=product.name)
            return row, row

        @after(add_product, df=audited)
        def audit(self, *, df, spark, ctx):
            return df

    text = pyspark.render.transform()(
        pyspark.plan.lower()(compile_transform(AddProduct)),
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
    assert "        audited = self._impl.audit(df=audited, spark=self.spark, ctx=self.ctx)" in text
    assert 'return TransformResult({"accepted": accepted_df, "audited": audited_df}, single=False)' in text

    traceability = build_compiler_traceability(
        pyspark.plan.lower()(compile_transform(AddProduct)),
        source_transform="tests.specifications.multiple_schema_parameters.AddProduct",
        transform_module="testing.generated.AddProductGenerated",
    )
    projections = [dependency for dependency in traceability.static_dataflow if dependency.operation == "project"]
    assert {dependency.detail.get("result") for dependency in projections} == {"accepted", "audited"}
    assert len([dependency for dependency in traceability.static_dataflow if dependency.operation == "step"]) == 1


def test_multi_result_after_hook_requires_df_selection() -> None:
    @transform
    class AddProduct(Transform):
        orders = input(OrderRaw)
        products = input(Product)
        accepted = output(OrderWithProduct)
        audited = output(OrderWithProduct)

        @transform(inputs=[orders, products], outputs=[accepted, audited])
        def add_product(
            self,
            order: OrderRaw,
            product: Product,
        ) -> tuple[OrderWithProduct, OrderWithProduct]:
            product = join_one(product, on=product.id == order.product_id)
            row = OrderWithProduct(id=order.id, product_name=product.name)
            return row, row

        @after(add_product)
        def audit(self, *, df, spark, ctx):
            return df

    with pytest.raises(Exception, match="is ambiguous"):
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
