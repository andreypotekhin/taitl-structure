"""Spark-free authoring rules for explicit Delta Z-order maintenance."""

import ast
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from structure import Schema, Transform, variable
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import delta_optimize, delta_table, string, struct
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.delta.model import DeltaMutation
from structure.plugin.pyspark.delta.operations import DeltaScope
from structure.plugin.pyspark.delta.runtime import bind_delta_predicate_variables, execute_delta_optimize
from structure.plugin.pyspark.render.commands.RenderPySparkTransformModule import render_pyspark_transform_module


class Customer(Schema):
    id = string()


class Order(Schema):
    customer_id = string(alias="customer-key")
    product_id = string()
    order_date = string()
    customer = struct(Customer)


def scope(name="orders", binding="delta_table"):
    return DeltaScope(name=name, schema=Order, source=name, binding=binding)


def test_zorder_retains_multiple_physical_keys_and_partition_variable() -> None:
    class ZOrderPartition(Transform):
        orders = delta_table(Order)
        selected_date = variable(str)

        def optimize(self, order: Order) -> None:
            delta_optimize(order, where=order.order_date == self.selected_date).execute_zorder(
                by=(order.customer_id, order.product_id),
            )

    plan = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(ZOrderPartition, materialize_schemas=False, plugin={"pyspark": {}}).lowered,
    )
    assert [step.name for step in plan.steps] == ["optimize"]
    assert plan.steps[0].effect
    mutation = plan.steps[0].delta_mutations[0]
    assert isinstance(mutation, DeltaMutation)
    assert mutation.action == "zorder"
    assert mutation.columns == ("customer-key", "product_id")
    assert mutation.predicate is not None
    assert mutation.predicate.args[1].kind == "variable"
    assert plan.outputs[0].binding == "delta_table"

    source = render_pyspark_transform_module(
        plan,
        source_transform=f"{__name__}.ZOrderPartition",
        schema_modules={Order: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    tree = ast.parse(source)
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "execute_delta_optimize"
    ]
    assert len(calls) == 1
    assert ast.literal_eval(calls[0].args[2]) == "zorder"
    assert ast.literal_eval(calls[0].args[3]) == ("customer-key", "product_id")
    assert ast.literal_eval(calls[0].args[4]) == ("order_date",)


@pytest.mark.parametrize(
    ("keys", "message"),
    [
        (lambda order: (), "non-empty tuple"),
        (lambda order: [order.customer_id], "non-empty tuple"),
        (lambda order: order.customer_id, "non-empty tuple"),
        (lambda order: (order.customer_id, order.customer_id), "duplicate fields"),
        (lambda order: (order.customer_id == "c1",), "only fields"),
        (lambda order: (order.customer.id,), "top-level fields"),
        (lambda order: (scope("other").customer_id,), "top-level fields"),
    ],
    ids=["empty", "list", "single-field", "duplicate", "expression", "nested", "foreign-table"],
)
def test_zorder_rejects_invalid_keys(keys, message) -> None:
    order = scope()
    with pytest.raises(TypeError, match=message):
        delta_optimize(order).execute_zorder(by=keys(order))


def test_generated_literal_partition_filter_needs_no_declared_variables() -> None:
    class ZOrderLiteral(Transform):
        orders = delta_table(Order)

        def optimize(self, order: Order) -> None:
            delta_optimize(order, where=order.order_date == "2026-10-08").execute_zorder(by=(order.product_id,))

    plan = Compiler.frontend.compile()(ZOrderLiteral, materialize_schemas=False, plugin={"pyspark": {}}).lowered
    source = render_pyspark_transform_module(
        plan,
        source_transform=f"{__name__}.ZOrderLiteral",
        schema_modules={Order: "tests.schemas"},
        runtime_module="tests.runtime",
    )
    bindings = [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "bind_delta_predicate_variables"
    ]
    assert len(bindings) == 1
    # Execute the rendered binding with the same variable-free instance shape as generated code.
    predicate = eval(
        compile(ast.Expression(bindings[0]), "<generated-optimize-predicate>", "eval"),
        {"self": SimpleNamespace(), "bind_delta_predicate_variables": bind_delta_predicate_variables},
    )
    assert predicate == "(`order_date` = '2026-10-08')"


def test_zorder_rejects_read_only_target() -> None:
    with pytest.raises(TypeError, match=r"requires a delta_table\(\.\.\.\) relation"):
        delta_optimize(scope(binding="delta_input"))


def test_zorder_builder_cannot_execute_twice(monkeypatch) -> None:
    context = SimpleNamespace(delta_mutations=[])
    monkeypatch.setattr("structure.plugin.pyspark.delta.operations.current_pyspark_context", lambda: context)
    order = scope()
    builder = delta_optimize(order)
    builder.execute_zorder(by=(order.customer_id,))
    with pytest.raises(TypeError, match="executed only once"):
        builder.execute_zorder(by=(order.product_id,))
    assert len(context.delta_mutations) == 1


def test_optimize_rejects_non_partition_predicate_before_native_builder() -> None:
    table = Mock()
    table.detail.return_value.first.return_value.asDict.return_value = {"partitionColumns": ["order_date"]}
    with pytest.raises(ValueError, match="only partition columns; invalid: customer-key"):
        execute_delta_optimize(table, "`customer-key` = 'c1'", "zorder", ("product_id",), ("customer-key",))
    table.optimize.assert_not_called()
