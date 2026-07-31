from typing import cast

import pytest

from structure import Schema, Transform, input, lane, output, step
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import (
    PySpark,
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


class ActiveItemWithNote(Schema):
    item_id = string(nullable=False)
    score = integer(nullable=False)
    note = string()


class ArchivedItemWithoutNote(Schema):
    item_id = string(nullable=False)
    score = integer(nullable=False)


class ArchivedItemIdOnly(Schema):
    item_id = string(nullable=False)


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


class MergeBranchedItems(Transform):
    active = input(ActiveItem)
    archived = input(ArchivedItem)
    scoped = lane(ActiveItem)
    global_context = lane(ArchivedItem)
    merged = output(ActiveItem)

    @step(input=active, output=scoped)
    def scope_active(self, active: ActiveItem) -> ActiveItem:
        return ActiveItem.project(active)

    @step(input=archived, output=global_context)
    def scope_global(self, archived: ArchivedItem) -> ArchivedItem:
        return ArchivedItem.project(archived)

    @step(input=[scoped, global_context], output=merged)
    def rejoin(self, scoped: ActiveItem, global_context: ArchivedItem) -> ActiveItem:
        merged = union_all(global_context)
        return ActiveItem.project(merged)


class MergeItemsByName(Transform):
    active = input(ActiveItem)
    archived = input(ArchivedItem)
    merged = output(ActiveItem)

    def merge(self, active: ActiveItem, archived: ArchivedItem) -> ActiveItem:
        merged = union_by_name(archived)
        return ActiveItem.project(merged)


class MergeItemsByNameWithMissingNullable(Transform):
    active = input(ActiveItemWithNote)
    archived = input(ArchivedItemWithoutNote)
    merged = output(ActiveItemWithNote)

    def merge(self, active: ActiveItemWithNote, archived: ArchivedItemWithoutNote) -> ActiveItemWithNote:
        merged = union_by_name(archived, allow_missing_columns=True)
        return ActiveItemWithNote.project(merged)


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


def test_union_all_rejoins_independently_materialized_lanes() -> None:
    """A branch can materialize typed lanes and rejoin them without an opaque hook."""

    step = _lowered(MergeBranchedItems).steps[2]
    operation = step.operations[0]

    assert step.source == "scoped"
    assert operation.kind == "union_all"
    assert operation.relation_set is not None
    assert operation.relation_set.input_name == "global_context"
    assert operation.relation_set.source == "global_context"


def test_generated_union_all_rejoins_branch_lane_sources() -> None:
    """Generated transforms route branch union through materialized lane frames."""

    text = PySpark.render.transform()(
        _lowered(MergeBranchedItems),
        source_transform="tests.specifications.v6_api_ledger.test_v6_relation_union.MergeBranchedItems",
        runtime_module="testing.runtime",
        schema_modules={ActiveItem: "testing.schemas", ArchivedItem: "testing.schemas"},
    )

    assert "        # Step method: scope_active" in text
    assert "        # Step method: scope_global" in text
    assert "        # Step method: rejoin" in text
    assert '        merged = scoped.alias("active_item")' in text
    assert "        merged = merged.union(global_context)" in text


def test_union_by_name_renders_exact_schema_name_aligned_union_source() -> None:
    text = render_pyspark_step(
        _lowered(MergeItemsByName).steps[0],
        current="active",
        sources={"active": "active", "archived": "archived"},
    )

    assert "active = active.unionByName(archived, allowMissingColumns=False)" in text


def test_union_by_name_records_nullable_missing_column_composition() -> None:
    operation = _lowered(MergeItemsByNameWithMissingNullable).steps[0].operations[0]

    assert operation.kind == "union_by_name"
    assert operation.relation_set is not None
    assert operation.relation_set.input_name == "archived"
    assert operation.relation_set.allow_missing_columns is True


def test_union_by_name_renders_missing_nullable_column_composition() -> None:
    text = render_pyspark_step(
        _lowered(MergeItemsByNameWithMissingNullable).steps[0],
        current="active",
        sources={"active": "active", "archived": "archived"},
    )

    assert "active = active.unionByName(archived, allowMissingColumns=True)" in text


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


def test_branchable_union_records_traceability_dependency_between_lanes() -> None:
    traceability = Compiler.traceability.build()(
        _lowered(MergeBranchedItems),
        source_transform="tests.MergeBranchedItems",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    dependency = dependencies["rejoin.union_all[0].global_context"]
    assert dependency.sources == ("scoped", "global_context")
    assert dependency.operation == "union_all"
    assert dependency.detail["source"] == "global_context"


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


def test_union_by_name_rejects_missing_non_nullable_columns() -> None:
    class BadMerge(Transform):
        active = input(ActiveItem)
        mismatched = input(ArchivedItemIdOnly)
        merged = output(ActiveItem)

        def merge(self, active: ActiveItem, mismatched: ArchivedItemIdOnly) -> ActiveItem:
            merged = union_by_name(mismatched, allow_missing_columns=True)
            return ActiveItem.project(merged)

    with pytest.raises(TypeError, match="non-null field\\(s\\) need defaults: score"):
        Compiler.frontend.compile()(BadMerge, materialize_schemas=False)


def test_union_by_name_rejects_defaults_until_fill_design_exists() -> None:
    class BadMerge(Transform):
        active = input(ActiveItemWithNote)
        archived = input(ArchivedItemWithoutNote)
        merged = output(ActiveItemWithNote)

        def merge(self, active: ActiveItemWithNote, archived: ArchivedItemWithoutNote) -> ActiveItemWithNote:
            merged = union_by_name(archived, allow_missing_columns=True, defaults={"note": "unknown"})
            return ActiveItemWithNote.project(merged)

    with pytest.raises(TypeError, match="defaults=.*design-gated"):
        Compiler.frontend.compile()(BadMerge, materialize_schemas=False)


def _lowered(transform: type[Transform]) -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(transform, materialize_schemas=False).lowered)
