from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import integer, repartition_by_range, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class RangeItem(Schema):
    item_id = string(nullable=False)
    score = integer(nullable=False)


class PartitionItems(Transform):
    items = input(RangeItem)
    partitioned = output(RangeItem)

    def partition(self, item: RangeItem) -> RangeItem:
        return RangeItem.project(repartition_by_range(4, item.score.desc(), item.item_id))


def test_range_partitioning_is_public_pyspark_api() -> None:
    assert repartition_by_range is not None


def test_range_partitioning_records_and_renders_operation() -> None:
    plan = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(PartitionItems, materialize_schemas=False).lowered,
    )
    operation = plan.steps[0].operations[0]

    assert operation.kind == "repartition_by_range"
    assert operation.relation_partition is not None
    assert operation.relation_partition.count == 4
    text = render_pyspark_step(plan.steps[0], current="items", sources={"items": "items"})
    assert "items = items.repartitionByRange(4, F.col(\"range_item.score\").desc(), F.col(\"range_item.item_id\").asc())" in text


def test_range_partitioning_explain_and_streaming_status() -> None:
    text = render_explain_report(PartitionItems)
    assert "repartition_by_range(row_preserving partitions=4 keys=2)" in text
    assert "status: compatible" in text


def test_range_partitioning_rejects_invalid_counts_and_missing_keys() -> None:
    class MissingKeys(Transform):
        items = input(RangeItem)
        partitioned = output(RangeItem)

        def partition(self, item: RangeItem) -> RangeItem:
            repartition_by_range(2)
            return item

    class InvalidCount(Transform):
        items = input(RangeItem)
        partitioned = output(RangeItem)

        def partition(self, item: RangeItem) -> RangeItem:
            repartition_by_range(0, item.score)
            return item

    with pytest.raises(TypeError, match="requires at least one order expression"):
        Compiler.frontend.compile()(MissingKeys, materialize_schemas=False)
    with pytest.raises(TypeError, match="positive integer count"):
        Compiler.frontend.compile()(InvalidCount, materialize_schemas=False)
