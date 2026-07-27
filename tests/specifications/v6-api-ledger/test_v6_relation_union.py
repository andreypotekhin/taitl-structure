from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import (
    except_all,
    integer,
    intersect,
    intersect_all,
    string,
    subtract,
    union_all,
    union_by_name,
)
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class ActiveItem(Schema):
    item_id = string(nullable=False)
    score = integer(nullable=False)


class ArchivedItem(Schema):
    item_id = string(nullable=False)
    score = integer(nullable=False)


class MismatchedItem(Schema):
    item_id = string(nullable=False)
    value = integer(nullable=False)


class MergeItems(Transform):
    active = input(ActiveItem)
    archived = input(ArchivedItem)
    merged = output(ActiveItem)

    def merge(self, active: ActiveItem, archived: ArchivedItem) -> ActiveItem:
        merged = union_all(archived)
        return ActiveItem.project(merged)


class MergeItemsByName(Transform):
    active = input(ActiveItem)
    archived = input(ArchivedItem)
    merged = output(ActiveItem)

    def merge(self, active: ActiveItem, archived: ArchivedItem) -> ActiveItem:
        merged = union_by_name(archived)
        return ActiveItem.project(merged)


class IntersectItems(Transform):
    active = input(ActiveItem)
    archived = input(ArchivedItem)
    merged = output(ActiveItem)

    def merge(self, active: ActiveItem, archived: ArchivedItem) -> ActiveItem:
        merged = intersect(archived)
        return ActiveItem.project(merged)


class IntersectAllItems(Transform):
    active = input(ActiveItem)
    archived = input(ArchivedItem)
    merged = output(ActiveItem)

    def merge(self, active: ActiveItem, archived: ArchivedItem) -> ActiveItem:
        merged = intersect_all(archived)
        return ActiveItem.project(merged)


class SubtractItems(Transform):
    active = input(ActiveItem)
    archived = input(ArchivedItem)
    merged = output(ActiveItem)

    def merge(self, active: ActiveItem, archived: ArchivedItem) -> ActiveItem:
        merged = subtract(archived)
        return ActiveItem.project(merged)


class ExceptAllItems(Transform):
    active = input(ActiveItem)
    archived = input(ArchivedItem)
    merged = output(ActiveItem)

    def merge(self, active: ActiveItem, archived: ArchivedItem) -> ActiveItem:
        merged = except_all(archived)
        return ActiveItem.project(merged)


def test_union_all_records_exact_schema_relation_operation() -> None:
    operation = _lowered(MergeItems).steps[0].operations[0]

    assert operation.kind == "union_all"
    assert operation.relation_set is not None
    assert operation.relation_set.input_name == "archived"
    assert operation.relation_set.schema is ArchivedItem
    assert operation.relation_set.by_name is False


@pytest.mark.parametrize(
    ("transform", "operation"),
    (
        (IntersectItems, "intersect"),
        (IntersectAllItems, "intersect_all"),
        (SubtractItems, "subtract"),
        (ExceptAllItems, "except_all"),
    ),
)
def test_set_operations_record_exact_schema_relation_operation(
    transform: type[Transform], operation: str
) -> None:
    recipe = _lowered(transform).steps[0].operations[0]

    assert recipe.kind == operation
    assert recipe.relation_set is not None
    assert recipe.relation_set.operation == operation
    assert recipe.relation_set.input_name == "archived"


def test_union_all_renders_public_pyspark_union_source() -> None:
    text = render_pyspark_step(
        _lowered(MergeItems).steps[0],
        current="active",
        sources={"active": "active", "archived": "archived"},
    )

    assert "active = active.union(archived)" in text
    assert 'F.col("item_id")' in text
    assert 'F.col("score")' in text


def test_union_by_name_renders_exact_schema_name_aligned_union_source() -> None:
    text = render_pyspark_step(
        _lowered(MergeItemsByName).steps[0],
        current="active",
        sources={"active": "active", "archived": "archived"},
    )

    assert "active = active.unionByName(archived, allowMissingColumns=False)" in text


@pytest.mark.parametrize(
    ("transform", "snippet"),
    (
        (IntersectItems, "active = active.intersect(archived)"),
        (IntersectAllItems, "active = active.intersectAll(archived)"),
        (SubtractItems, "active = active.subtract(archived)"),
        (ExceptAllItems, "active = active.exceptAll(archived)"),
    ),
)
def test_set_operations_render_public_pyspark_set_sources(
    transform: type[Transform], snippet: str
) -> None:
    text = render_pyspark_step(
        _lowered(transform).steps[0],
        current="active",
        sources={"active": "active", "archived": "archived"},
    )

    assert snippet in text


def test_relation_union_explain_names_cardinality_and_streaming_status() -> None:
    text = render_explain_report(MergeItems)

    assert "operations: union_all(row_multiplying input=archived schema=ArchivedItem)" in text
    assert "STREAM-E0801: batch_only in merge (union_all archived)" in text


def test_relation_set_explain_names_filtering_cardinality_and_streaming_status() -> None:
    text = render_explain_report(IntersectItems)

    assert "operations: intersect(row_filtering input=archived schema=ArchivedItem)" in text
    assert "STREAM-E0801: batch_only in merge (intersect archived)" in text


def test_relation_union_records_traceability_dependency() -> None:
    traceability = Compiler.traceability.build()(
        _lowered(MergeItems),
        source_transform="tests.MergeItems",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    dependency = dependencies["merge.union_all[0].archived"]
    assert dependency.sources == ("active", "archived")
    assert dependency.operation == "union_all"
    assert dependency.detail["schema"] == "ArchivedItem"


def test_relation_union_rejects_unaligned_schemas() -> None:
    class BadMerge(Transform):
        active = input(ActiveItem)
        mismatched = input(MismatchedItem)
        merged = output(ActiveItem)

        def merge(self, active: ActiveItem, mismatched: MismatchedItem) -> ActiveItem:
            merged = union_all(mismatched)
            return ActiveItem.project(merged)

    with pytest.raises(TypeError, match="requires identical declared schemas"):
        Compiler.frontend.compile()(BadMerge, materialize_schemas=False)


def _lowered(transform: type[Transform]) -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(transform, materialize_schemas=False).lowered)
