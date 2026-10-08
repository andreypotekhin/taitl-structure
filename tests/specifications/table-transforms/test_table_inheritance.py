"""Inheritance contracts for caller-bound Delta and Iceberg tables."""

from typing import cast

import pytest

from structure import Schema, Transform, input, output, transform
from structure.core.compiler.api import Compiler
from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark import (
    delta_delete,
    delta_input,
    delta_output,
    delta_table,
    iceberg_delete,
    iceberg_input,
    iceberg_output,
    iceberg_table,
    integer,
    string,
)
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


class Order(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)


def _compile(transform_class):
    return cast(TransformPlan, Compiler.frontend.compile()(transform_class, materialize_schemas=False).analysis)


def _recipe(transform_class):
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(transform_class, materialize_schemas=False).lowered)


@pytest.mark.parametrize(
    ("table", "delete"),
    [(delta_table, delta_delete), (iceberg_table, iceberg_delete)],
)
def test_inherited_table_effect_can_be_replaced_in_place(table, delete) -> None:
    class Base(Transform):
        orders = table(Order)

        def clean(self, order: Order) -> None:
            delete(order, where=order.status == "legacy")

    @transform
    class Child(Base):
        def clean(self, order: Order) -> None:
            delete(order, where=order.status == "archived")

    plan = _recipe(Child)

    assert [step.name for step in plan.steps] == ["clean"]
    assert len(plan.steps) == 1
    assert plan.steps[0].effect
    assert len(plan.steps[0].delta_mutations) == 1
    assert plan.steps[0].delta_mutations[0].predicate is not None


@pytest.mark.parametrize(
    ("table", "delete", "provider"),
    [(delta_table, delta_delete, "delta"), (iceberg_table, iceberg_delete, "iceberg")],
)
def test_super_schedules_parent_table_effect_before_child(table, delete, provider) -> None:
    class Base(Transform):
        orders = table(Order)

        def clean(self, order: Order) -> None:
            delete(order, where=order.status == "legacy")

    @transform
    class Child(Base):
        def clean(self, order: Order) -> None:
            super().clean(order)
            delete(order, where=order.status == "archived")

    plan = _recipe(Child)

    assert [step.name for step in plan.steps] == ["Base.clean", "clean"]
    assert [step.delta_mutations[0].kind for step in plan.steps] == [
        "delete" if provider == "delta" else "iceberg_delete",
        "delete" if provider == "delta" else "iceberg_delete",
    ]
    assert [step.delta_mutations[0].target for step in plan.steps] == ["orders", "orders"]


@pytest.mark.parametrize(
    ("base_binding", "child_binding"),
    [
        (input, delta_input),
        (input, iceberg_input),
        (delta_input, input),
        (iceberg_input, input),
        (delta_input, iceberg_input),
        (iceberg_input, delta_input),
        (delta_input, delta_table),
        (delta_table, delta_input),
        (iceberg_input, iceberg_table),
        (iceberg_table, iceberg_input),
    ],
)
def test_child_cannot_change_an_inherited_input_binding_kind(base_binding, child_binding) -> None:
    class Base(Transform):
        rows = base_binding(Order)

    with pytest.raises((StructureCompileError, TypeError), match="binding|role|inherit|table|input"):

        @transform
        class Child(Base):
            rows = child_binding(Order)

            def expose(self, order: Order) -> Order:
                return Order(id=order.id, status=order.status)

        _compile(Child)


@pytest.mark.parametrize(
    ("base_binding", "child_binding"),
    [(output, delta_output), (output, iceberg_output), (delta_output, output), (iceberg_output, output)],
)
def test_child_cannot_change_an_inherited_output_binding_kind(base_binding, child_binding) -> None:
    class Base(Transform):
        rows = input(Order)
        result = base_binding(Order)

    with pytest.raises((StructureCompileError, TypeError), match="binding|role|inherit|table|output"):

        @transform
        class Child(Base):
            result = child_binding(Order)

            def project(self, order: Order) -> Order:
                return Order(id=order.id, status=order.status)

        _compile(Child)


@pytest.mark.parametrize("table", [delta_table, iceberg_table])
def test_child_cannot_redeclare_an_inherited_table_with_a_new_schema(table) -> None:
    class OrderV2(Order):
        note = string()

    class Base(Transform):
        orders = table(Order)

    with pytest.raises((StructureCompileError, TypeError), match="schema|inherit|table"):

        @transform
        class Child(Base):
            orders = table(OrderV2)

            def clean(self, order: OrderV2) -> None:
                return None

        _compile(Child)


@pytest.mark.parametrize(
    ("table", "provider"),
    [(delta_table, "delta"), (iceberg_table, "iceberg")],
)
def test_super_can_forward_a_table_result_without_creating_an_effect(table, provider: str) -> None:
    class Base(Transform):
        orders = table(Order)

        def identity(self, order: Order) -> Order:
            return order

    @transform
    class Child(Base):
        def identity(self, order: Order) -> Order:
            return super().identity(order)

    plan = _recipe(Child)

    assert [step.name for step in plan.steps] == ["Base.identity", "identity"]
    assert all(step.table_forward and not step.effect for step in plan.steps)
    assert all(step.delta_mutations == () for step in plan.steps)
    assert all(step.results[0].binding == f"{provider}_table" for step in plan.steps)
    assert all(step.results[0].table_source == "orders" for step in plan.steps)


@pytest.mark.parametrize(
    ("table", "delete", "provider"),
    [(delta_table, delta_delete, "delta"), (iceberg_table, iceberg_delete, "iceberg")],
)
def test_pure_super_delegation_keeps_only_the_parent_effect(table, delete, provider: str) -> None:
    class Base(Transform):
        orders = table(Order)

        def clean(self, order: Order) -> None:
            delete(order, where=order.status == "legacy")

    @transform
    class Child(Base):
        def clean(self, order: Order) -> None:
            super().clean(order)

    plan = _recipe(Child)

    assert [step.name for step in plan.steps] == ["Base.clean", "clean"]
    parent, child = plan.steps
    assert parent.effect and len(parent.delta_mutations) == 1
    assert child.table_forward and not child.effect and child.delta_mutations == ()
    assert parent.results[0].binding == child.results[0].binding == f"{provider}_table"
    assert parent.results[0].table_source == child.results[0].table_source == "orders"
