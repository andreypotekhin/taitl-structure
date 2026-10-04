from typing import cast

import pytest

from structure import Schema, Transform, input, output, transform
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import integer, stack, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class StackInput(Schema):
    item_id = integer(nullable=False)
    label = string(nullable=False)


class StackOutput(Schema):
    item_id = integer(nullable=False, alias="stacked_id")
    label = string(nullable=True, alias="stacked_label")


class StackItems(Transform):
    items = input(StackInput)
    expanded = output(StackOutput)

    def expand(self, row: StackInput) -> StackOutput:
        return stack(2, row.item_id, row.label, row.item_id + 1, to=StackOutput)


@transform(streaming=True)
class StreamingStackItems(Transform):
    items = input(StackInput, streaming=True)
    expanded = output(StackOutput)

    def expand(self, row: StackInput) -> StackOutput:
        return stack(2, row.item_id, row.label, row.item_id + 1, to=StackOutput)


def test_stack_is_public_and_lowers_as_fixed_row_expansion() -> None:
    assert stack is not None
    plan = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(StackItems, materialize_schemas=False).lowered,
    )
    operation = plan.steps[0].operations[0]

    assert operation.kind == "stack"
    assert operation.stack is not None
    assert operation.stack.rows == 2
    assert len(operation.stack.values) == 3
    text = render_pyspark_step(plan.steps[0], current="items", sources={"items": "items"})
    assert 'F.stack(F.lit(2), F.col("stack_input.item_id"), F.col("stack_input.label"), ' in text
    assert "__add__" not in text
    assert "stack(row_multiplying rows=2 values=3)" in render_explain_report(StackItems)


def test_stack_requires_positive_rows_and_nullable_trailing_padding() -> None:
    class InvalidRows(Transform):
        items = input(StackInput)
        expanded = output(StackOutput)

        def expand(self, row: StackInput) -> StackOutput:
            return stack(0, row.item_id, to=StackOutput)

    class NonNullablePadding(Schema):
        item_id = integer(nullable=False)
        label = string(nullable=False)

    class InvalidPadding(Transform):
        items = input(StackInput)
        expanded = output(NonNullablePadding)

        def expand(self, row: StackInput) -> NonNullablePadding:
            return stack(2, row.item_id, row.label, row.item_id, to=NonNullablePadding)

    with pytest.raises(TypeError, match="positive integer row count"):
        Compiler.frontend.compile()(InvalidRows, materialize_schemas=False)
    with pytest.raises(TypeError, match="must be nullable"):
        Compiler.frontend.compile()(InvalidPadding, materialize_schemas=False)


def test_stack_requires_exact_output_shape_and_position_types() -> None:
    class WrongType(Schema):
        item_id = string(nullable=False)
        label = string(nullable=True)

    class InvalidSchema(Transform):
        items = input(StackInput)
        expanded = output(WrongType)

        def expand(self, row: StackInput) -> WrongType:
            return stack(2, row.item_id, row.label, row.item_id + 1, to=WrongType)

    with pytest.raises(TypeError, match="must have type integer"):
        Compiler.frontend.compile()(InvalidSchema, materialize_schemas=False)


def test_stack_is_streaming_compatible_by_design() -> None:
    report = render_explain_report(StreamingStackItems)

    assert "status: compatible" in report
    assert "stack(row_multiplying rows=2 values=3)" in report
