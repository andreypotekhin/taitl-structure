from typing import Any, cast

import pytest

from structure import Schema, Transform, input, output, transform
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import repartition, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class RepartitionRow(Schema):
    customer_id = string(nullable=False)


class CountOnly(Transform):
    rows = input(RepartitionRow)
    result = output(RepartitionRow)

    def distribute(self, row: RepartitionRow) -> RepartitionRow:
        repartition(8)
        return row


class CountAndKey(Transform):
    rows = input(RepartitionRow)
    result = output(RepartitionRow)

    def distribute(self, row: RepartitionRow) -> RepartitionRow:
        repartition(8, row.customer_id)
        return row


class KeysOnly(Transform):
    rows = input(RepartitionRow)
    result = output(RepartitionRow)

    def distribute(self, row: RepartitionRow) -> RepartitionRow:
        repartition(row.customer_id)
        return row


@transform(streaming=True)
class StreamingRepartition(Transform):
    rows = input(RepartitionRow, streaming=True)
    result = output(RepartitionRow)

    def distribute(self, row: RepartitionRow) -> RepartitionRow:
        repartition(8, row.customer_id)
        return row


def _plan(transform_type: type[Transform]) -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(transform_type, materialize_schemas=False).lowered,
    )


def test_repartition_supports_count_keys_and_combined_forms() -> None:
    count_plan = _plan(CountOnly)
    count_operation = count_plan.steps[0].operations[0]
    assert count_operation.kind == "repartition"
    assert count_operation.relation_repartition is not None
    assert count_operation.relation_repartition.partitions == 8
    assert count_operation.relation_repartition.keys == ()

    combined = _plan(CountAndKey).steps[0].operations[0].relation_repartition
    assert combined is not None
    assert combined.partitions == 8
    assert len(combined.keys) == 1

    keys_only = _plan(KeysOnly).steps[0].operations[0].relation_repartition
    assert keys_only is not None
    assert keys_only.partitions is None
    assert len(keys_only.keys) == 1

    code = render_pyspark_step(count_plan.steps[0], current="rows", sources={"rows": "rows"})
    assert "rows = rows.repartition(8)" in code
    assert "repartition(row_preserving partitions=8 keys=0)" in render_explain_report(CountOnly)


def test_repartition_rejects_missing_or_invalid_partition_count() -> None:
    untyped_repartition = cast(Any, repartition)

    class NoArguments(Transform):
        rows = input(RepartitionRow)
        result = output(RepartitionRow)

        def distribute(self, row: RepartitionRow) -> RepartitionRow:
            untyped_repartition()
            return row

    class InvalidCount(Transform):
        rows = input(RepartitionRow)
        result = output(RepartitionRow)

        def distribute(self, row: RepartitionRow) -> RepartitionRow:
            untyped_repartition(0)
            return row

    class BooleanCount(Transform):
        rows = input(RepartitionRow)
        result = output(RepartitionRow)

        def distribute(self, row: RepartitionRow) -> RepartitionRow:
            untyped_repartition(True)
            return row

    with pytest.raises(TypeError, match="requires a positive count or at least one partition key"):
        Compiler.frontend.compile()(NoArguments, materialize_schemas=False)
    with pytest.raises(TypeError, match="positive integer partition count"):
        Compiler.frontend.compile()(InvalidCount, materialize_schemas=False)
    with pytest.raises(TypeError, match="positive integer partition count"):
        Compiler.frontend.compile()(BooleanCount, materialize_schemas=False)


def test_streaming_repartition_is_compatible_with_throughput_advisory() -> None:
    report = render_explain_report(StreamingRepartition)

    assert "status: compatible" in report
    assert "STREAM-W0804" in report
    assert "throughput" in report
