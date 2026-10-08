from typing import cast

from structure import Schema, Transform, transform
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import iceberg_delete, iceberg_table, integer, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class Order(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)


class Base(Transform):
    orders = iceberg_table(Order)

    def clean(self, order: Order) -> None:
        iceberg_delete(order, where=order.status == "legacy")


@transform
class Replace(Base):
    def clean(self, order: Order) -> None:
        iceberg_delete(order, where=order.status == "archived")


@transform
class Extend(Base):
    def clean(self, order: Order) -> None:
        super().clean(order)
        iceberg_delete(order, where=order.status == "archived")


def test_iceberg_inherited_method_replacement_and_parent_extension() -> None:
    compile_transform = Compiler.frontend.compile()
    replacement = cast(PySparkExecutionPlan, compile_transform(Replace, materialize_schemas=False).lowered)
    extension = cast(PySparkExecutionPlan, compile_transform(Extend, materialize_schemas=False).lowered)

    assert [step.name for step in replacement.steps] == ["clean"]
    assert [step.name for step in extension.steps] == ["Base.clean", "clean"]
    assert [len(step.delta_mutations) for step in extension.steps] == [1, 1]
    assert all(step.table_inputs[0].table_source == "orders" for step in extension.steps)
