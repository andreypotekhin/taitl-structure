from typing import Any, cast

import pytest

from structure import Schema, Transform, input, output, transform
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import coalesce, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class CoalesceItem(Schema):
    item_id = string(nullable=False)


class CoalesceItems(Transform):
    items = input(CoalesceItem)
    fewer_partitions = output(CoalesceItem)

    def coalesce_items(self, item: CoalesceItem) -> CoalesceItem:
        coalesce(partitions=4)
        return item


@transform(streaming=True)
class StreamingCoalesceItems(Transform):
    items = input(CoalesceItem, streaming=True)
    fewer_partitions = output(CoalesceItem)

    def coalesce_items(self, item: CoalesceItem) -> CoalesceItem:
        coalesce(partitions=2)
        return item


def test_scalar_coalesce_requires_at_least_two_values() -> None:
    untyped_coalesce = cast(Any, coalesce)
    with pytest.raises(TypeError, match="at least two values"):
        untyped_coalesce("only-one")
    with pytest.raises(TypeError, match="at least two scalar values"):
        untyped_coalesce()


def test_relation_coalesce_uses_keyword_only_partitions_and_renders() -> None:
    plan = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(CoalesceItems, materialize_schemas=False).lowered,
    )
    operation = plan.steps[0].operations[0]

    assert operation.kind == "coalesce"
    assert operation.relation_coalesce == 4
    text = render_pyspark_step(plan.steps[0], current="items", sources={"items": "items"})
    assert "items = items.coalesce(4)" in text
    assert "coalesce(row_preserving partitions=4)" in render_explain_report(CoalesceItems)


def test_relation_coalesce_validates_partition_count_and_separation() -> None:
    class PositionalCount(Transform):
        items = input(CoalesceItem)
        result = output(CoalesceItem)

        def process(self, item: CoalesceItem) -> CoalesceItem:
            cast(Any, coalesce)(4)
            return item

    class InvalidCount(Transform):
        items = input(CoalesceItem)
        result = output(CoalesceItem)

        def process(self, item: CoalesceItem) -> CoalesceItem:
            coalesce(partitions=0)
            return item

    class MixedArguments(Transform):
        items = input(CoalesceItem)
        result = output(CoalesceItem)

        def process(self, item: CoalesceItem) -> CoalesceItem:
            cast(Any, coalesce)("fallback", "other", partitions=4)
            return item

    with pytest.raises(TypeError, match="at least two values"):
        Compiler.frontend.compile()(PositionalCount, materialize_schemas=False)
    with pytest.raises(TypeError, match="positive integer partition count"):
        Compiler.frontend.compile()(InvalidCount, materialize_schemas=False)
    with pytest.raises(TypeError, match="cannot be combined"):
        Compiler.frontend.compile()(MixedArguments, materialize_schemas=False)


def test_streaming_relation_coalesce_is_compatible_with_advisory() -> None:
    report = render_explain_report(StreamingCoalesceItems)

    assert "status: compatible" in report
    assert "STREAM-W0803" in report
    assert "throughput" in report
