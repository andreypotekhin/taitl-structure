"""Provider identity and role contracts for composed table transforms."""

from typing import cast

import pytest

from structure import Schema, Transform, output, transform
from structure.core.compiler.api import Compiler
from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.core.compiler.ir.model.TransformPlan import TransformPlan
from structure.plugin.pyspark import (
    delta_delete,
    delta_input,
    delta_table,
    iceberg_delete,
    iceberg_input,
    iceberg_table,
    integer,
    string,
)
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class Order(Schema):
    id = integer(nullable=False)
    status = string(nullable=False)


def _stages(provider: str):
    if provider == "delta":
        table, read, delete = delta_table, delta_input, delta_delete
    else:
        table, read, delete = iceberg_table, iceberg_input, iceberg_delete

    @transform
    class Writer(Transform):
        orders = table(Order)

        def clean(self, row: Order) -> None:
            delete(row, where=row.status == "old")

    @transform
    class Reader(Transform):
        orders = read(Order)
        selected = output(Order)

        def keep(self, row: Order) -> Order:
            return Order(id=row.id, status=row.status)

    return Writer, Reader


@pytest.mark.parametrize("provider", ["delta", "iceberg"])
def test_pipeline_handoff_preserves_provider_role_and_caller_table(provider: str) -> None:
    writer, reader = _stages(provider)

    plan = cast(
        TransformPlan,
        Compiler.frontend.compile()(writer(orders=object()).to(reader()), materialize_schemas=False).analysis,
    )

    assert [(item.name, item.binding) for item in plan.inputs] == [("orders", f"{provider}_table")]
    consumer = next(step for step in plan.steps if step.name.endswith(".keep"))
    table_input = consumer.inputs[0]
    assert table_input.binding == f"{provider}_input"
    assert table_input.table_source == "orders"
    producer = next(step for step in plan.steps if step.name.endswith(".clean"))
    mutation = cast(PySparkStepBody, producer.plugin_body).delta_mutations[0]
    assert mutation.target == "orders"


@pytest.mark.parametrize("provider", ["delta", "iceberg"])
def test_graph_handoff_preserves_provider_role_and_caller_table(provider: str) -> None:
    writer, reader = _stages(provider)
    table = delta_table if provider == "delta" else iceberg_table

    class Graph(Transform):
        orders = table(Order)
        cleaned = writer(orders=orders)
        selected = reader(orders=cleaned.orders)
        result = output(Order, selected.selected)

    plan = cast(TransformPlan, Compiler.frontend.compile()(Graph, materialize_schemas=False).analysis)

    assert [(item.name, item.binding) for item in plan.inputs] == [("orders", f"{provider}_table")]
    producer = next(step for step in plan.steps if step.name.startswith("cleaned."))
    consumer = next(step for step in plan.steps if step.name.startswith("selected."))
    assert cast(PySparkStepBody, producer.plugin_body).delta_mutations[0].target == "orders"
    assert consumer.inputs[0].binding == f"{provider}_input"
    assert consumer.inputs[0].table_source == "orders"
    assert plan.stage_outputs[0].output.binding == f"{provider}_table"
    assert plan.stage_outputs[0].output.table_source == "orders"


def test_pipeline_rejects_delta_to_iceberg_handoff() -> None:
    writer, _ = _stages("delta")
    _, iceberg_reader = _stages("iceberg")

    with pytest.raises(StructureCompileError, match="provider"):
        Compiler.frontend.compile()(
            writer(orders=object()).to(iceberg_reader()), materialize_schemas=False
        )


def test_read_only_wrapper_input_cannot_supply_mutable_stage_role() -> None:
    writer, _ = _stages("delta")

    class Wrapper(Transform):
        orders = delta_input(Order)
        stage = writer(orders=orders)
        result = output(Order, stage.orders)

    with pytest.raises(StructureCompileError, match="read-only|mutable"):
        Compiler.frontend.compile()(Wrapper, materialize_schemas=False)
