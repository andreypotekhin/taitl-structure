from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin import pyspark
from structure.plugin.pyspark import integer, limit, offset, order_by, string, union_all
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class RankedItem(Schema):
    item_id = string(nullable=False)
    score = integer(nullable=False)


class PublishTopItems(Transform):
    items = input(RankedItem)
    top = output(RankedItem)

    def rank(self, item: RankedItem) -> RankedItem:
        order_by(item.score.desc(), item.item_id)
        limit(10)
        page = offset(2)
        return RankedItem.project(page)


def test_relation_order_and_bounds_are_public_pyspark_api() -> None:
    assert pyspark.order_by is order_by
    assert pyspark.limit is limit
    assert pyspark.offset is offset


def test_relation_order_and_bounds_record_compiler_visible_operations() -> None:
    operations = _lowered().steps[0].operations

    assert [operation.kind for operation in operations] == ["order_by", "limit", "offset"]
    assert operations[0].relation_order is not None
    assert len(operations[0].relation_order.order_by) == 2
    assert operations[1].relation_bound is not None
    assert operations[1].relation_bound.count == 10
    assert operations[2].relation_bound is not None
    assert operations[2].relation_bound.count == 2


def test_relation_order_and_bounds_render_public_pyspark_sources() -> None:
    text = render_pyspark_step(_lowered().steps[0], current="items", sources={"items": "items"})

    assert (
        'items = items.orderBy(F.col("ranked_item.score").desc(), F.col("ranked_item.item_id").asc())'
        in text
    )
    assert "items = items.limit(10)" in text
    assert "items = items.offset(2)" in text


def test_relation_order_explain_names_cardinality_and_streaming_status() -> None:
    text = render_explain_report(PublishTopItems)

    assert (
        "operations: order_by(row_preserving keys=2), "
        "limit(row_filtering count=10), offset(row_filtering count=2)"
    ) in text
    assert "status: compatible" in text


def test_relation_order_and_bounds_record_traceability_dependencies() -> None:
    traceability = Compiler.traceability.build()(
        _lowered(),
        source_transform="tests.PublishTopItems",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    order = dependencies["rank.order_by[0]"]
    assert order.sources == ("items.score", "items.item_id")
    assert order.operation == "order_by"
    assert order.detail["order_by"] == 2

    bound = dependencies["rank.limit[1]"]
    assert bound.sources == ("items",)
    assert bound.operation == "limit"
    assert bound.detail["count"] == 10


def test_relation_bounds_reject_unordered_and_invalid_counts() -> None:
    class UnorderedLimit(Transform):
        items = input(RankedItem)
        top = output(RankedItem)

        def rank(self, item: RankedItem) -> RankedItem:
            return RankedItem.project(limit(1))

    class NegativeLimit(Transform):
        items = input(RankedItem)
        top = output(RankedItem)

        def rank(self, item: RankedItem) -> RankedItem:
            order_by(item.score)
            return RankedItem.project(limit(-1))

    class BooleanOffset(Transform):
        items = input(RankedItem)
        top = output(RankedItem)

        def rank(self, item: RankedItem) -> RankedItem:
            order_by(item.score)
            return RankedItem.project(offset(True))

    with pytest.raises(TypeError, match="requires order_by"):
        Compiler.frontend.compile()(UnorderedLimit, materialize_schemas=False)
    with pytest.raises(TypeError, match="non-negative integer literal"):
        Compiler.frontend.compile()(NegativeLimit, materialize_schemas=False)
    with pytest.raises(TypeError, match="non-negative integer literal"):
        Compiler.frontend.compile()(BooleanOffset, materialize_schemas=False)


def test_relation_bound_rejects_order_destroyed_by_set_operation() -> None:
    class OrderedThenUnioned(Transform):
        items = input(RankedItem)
        archived = input(RankedItem)
        top = output(RankedItem)

        def rank(self, item: RankedItem, archived: RankedItem) -> RankedItem:
            order_by(item.score)
            union_all(archived)
            return RankedItem.project(limit(1))

    with pytest.raises(TypeError, match="current relation state"):
        Compiler.frontend.compile()(OrderedThenUnioned, materialize_schemas=False)


def _lowered() -> PySparkExecutionPlan:
    return cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(PublishTopItems, materialize_schemas=False).lowered,
    )
