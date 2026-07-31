from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin import pyspark
from structure.plugin.pyspark import integer, limit, order_by, sample, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class Item(Schema):
    item_id = string(nullable=False)
    score = integer(nullable=False)


class SampleItems(Transform):
    items = input(Item)
    sampled = output(Item)

    def pick(self, item: Item) -> Item:
        picked = sample(0.25, seed=17)
        return Item.project(picked)


class UnseededSampleItems(Transform):
    items = input(Item)
    sampled = output(Item)

    def pick(self, item: Item) -> Item:
        picked = sample(1.5, with_replacement=True, reproducible=False)
        return Item.project(picked)


def test_sample_is_public_pyspark_api() -> None:
    assert pyspark.sample is sample


def test_sample_records_compiler_visible_operation() -> None:
    operation = _lowered(SampleItems).steps[0].operations[0]

    assert operation.kind == "sample"
    assert operation.relation_sample is not None
    assert operation.relation_sample.fraction == 0.25
    assert operation.relation_sample.with_replacement is False
    assert operation.relation_sample.seed == 17
    assert operation.relation_sample.reproducible is True


def test_sample_renders_public_pyspark_sample_source() -> None:
    text = render_pyspark_step(_lowered(SampleItems).steps[0], current="items", sources={"items": "items"})

    assert "items = items.sample(withReplacement=False, fraction=0.25, seed=17)" in text


def test_unseeded_sample_requires_explicit_non_reproducible_opt_in() -> None:
    text = render_pyspark_step(
        _lowered(UnseededSampleItems).steps[0],
        current="items",
        sources={"items": "items"},
    )

    assert "items = items.sample(withReplacement=True, fraction=1.5)" in text
    assert "seed=" not in text


def test_sample_explain_names_cardinality_reproducibility_and_streaming_status() -> None:
    text = render_explain_report(SampleItems)

    assert "operations: sample(row_filtering fraction=0.25 with_replacement=False seed=17)" in text
    assert "STREAM-E0801: batch_only in pick (sample)" in text


def test_sample_records_traceability_dependency() -> None:
    traceability = Compiler.traceability.build()(
        _lowered(SampleItems),
        source_transform="tests.SampleItems",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    dependency = dependencies["pick.sample[0]"]
    assert dependency.sources == ("items",)
    assert dependency.operation == "sample"
    assert dependency.detail["fraction"] == 0.25
    assert dependency.detail["with_replacement"] is False
    assert dependency.detail["seed"] == 17
    assert dependency.detail["reproducible"] is True


@pytest.mark.parametrize(
    ("call", "message"),
    (
        (lambda: sample(-0.1, seed=1), "must be in \\[0, 1\\]"),
        (lambda: sample(1.1, seed=1), "must be in \\[0, 1\\]"),
        (lambda: sample(-0.1, with_replacement=True, seed=1), "must be non-negative"),
        (lambda: sample(0.5), "seed=.*required"),
        (lambda: sample(0.5, seed=True), "integer literal"),
        (lambda: sample(0.5, with_replacement="yes", seed=1), "requires a Boolean"),  # type: ignore[arg-type]
        (lambda: sample(0.5, seed=1, reproducible="yes"), "requires a Boolean"),  # type: ignore[arg-type]
    ),
)
def test_sample_rejects_invalid_arguments(call, message: str) -> None:
    class BadSample(Transform):
        items = input(Item)
        sampled = output(Item)

        def pick(self, item: Item) -> Item:
            picked = call()
            return Item.project(picked)

    with pytest.raises(TypeError, match=message):
        Compiler.frontend.compile()(BadSample, materialize_schemas=False)


def test_sample_breaks_ordered_relation_state() -> None:
    class OrderedSampleBound(Transform):
        items = input(Item)
        sampled = output(Item)

        def pick(self, item: Item) -> Item:
            order_by(item.score)
            sample(0.5, seed=17)
            picked = limit(2)
            return Item.project(picked)

    with pytest.raises(TypeError, match="current relation state"):
        Compiler.frontend.compile()(OrderedSampleBound, materialize_schemas=False)


def _lowered(transform: type[Transform]) -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(transform, materialize_schemas=False).lowered)
