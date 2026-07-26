import pytest

import structure
from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *


def _analysis(transform):
    return Compiler.frontend.compile()(transform, materialize_schemas=False).analysis


def test_hooks_attach_to_declared_step_method_boundaries(orders_recipe) -> None:
    """I can attach a hook by placing @raw in source order among step methods."""

    assert [hook.name for hook in orders_recipe.steps[0].before_hooks] == ["use_current_orders"]
    assert [hook.name for hook in orders_recipe.steps[0].after_hooks] == ["remove_negative_totals"]
    assert [hook.name for hook in orders_recipe.steps[3].after_hooks] == ["note_lookup_inputs"]
    assert [hook.name for hook in orders_recipe.steps[4].after_hooks] == ["add_quality_columns"]


def test_hooks_record_explicit_input_access_and_projection_validation_contracts(orders_recipe) -> None:
    """I can see each raw parameter's resolved frame source."""

    lookup = orders_recipe.steps[3].after_hooks[0]
    quality = orders_recipe.steps[4].after_hooks[0]

    assert lookup.lanes == ("orders", "customers", "products")
    assert lookup.sources == ("orders", "input:customers", "input:products")
    assert lookup.schema_mode is SchemaMode.ALLOW_EXTRA_COLUMNS
    assert lookup.project_output
    assert quality.project_output
    assert [
        (validation.reason, validation.mode, validation.project) for validation in orders_recipe.steps[3].validations
    ] == [
        ("hook", SchemaMode.ALLOW_EXTRA_COLUMNS, True),
        ("hook_projected", SchemaMode.STRICT, False),
        ("intermediate", SchemaMode.STRICT, False),
    ]
    assert [
        (validation.reason, validation.mode, validation.project) for validation in orders_recipe.steps[4].validations
    ] == [
        ("hook", SchemaMode.ALLOW_EXTRA_COLUMNS, True),
        ("hook_projected", SchemaMode.STRICT, False),
    ]


def test_hooks_record_target_metadata() -> None:
    """Hooks carry target metadata through the PySpark recipe."""

    class Row(Schema):
        id = string(nullable=False)

    @transform
    class NormalizeRows(Transform):
        rows = input(Row)
        normalized = output(Row)

        @raw(inout=input(rows) | lane(rows), target=["pyspark"])
        def prepare(self, *, rows, spark, ctx):
            return rows

        def normalize(self, row: Row) -> Row:
            return Row(id=row.id)

        @raw(target="pyspark")
        def clean(self, *, rows, spark, ctx):
            return rows

    plan = _analysis(NormalizeRows)

    assert plan.steps[0].before_hooks[0].targets == ("pyspark",)
    assert not plan.steps[0].before_hooks[0].target_defaulted
    assert plan.steps[0].after_hooks[0].targets == ("pyspark",)
    assert not plan.steps[0].after_hooks[0].target_defaulted


def test_non_pyspark_only_hook_target_fails_before_runtime() -> None:
    """V1 accepts hook target syntax, but active execution is still PySpark only."""

    class Row(Schema):
        id = string(nullable=False)

    @transform
    class NormalizeRows(Transform):
        rows = input(Row)
        normalized = output(Row)

        def normalize(self, row: Row) -> Row:
            return Row(id=row.id)

        @raw(target="polars")
        def clean(self, *, rows, spark, ctx):
            return rows

    with pytest.raises(StructureCompileError) as raised:
        _analysis(NormalizeRows)

    assert "targets polars" in str(raised.value)
    assert "PySpark only" in str(raised.value)


def test_generated_code_calls_source_transform_hooks_directly(orders_transform_text) -> None:
    """Generated code calls hooks directly on the source transform instance."""

    assert "        self._impl = EnrichOrders()" in orders_transform_text
    assert "        orders = self._impl.use_current_orders(" in orders_transform_text
    assert "        orders = self._impl.remove_negative_totals(" in orders_transform_text
    assert "        orders = self._impl.note_lookup_inputs(" in orders_transform_text
    assert "        published = self._impl.add_quality_columns(" in orders_transform_text


def test_raw_methods_attach_in_declaration_order_after_the_preceding_step() -> None:
    """I can place a raw native-frame method between normal transform steps."""

    class Row(Schema):
        id = string(nullable=False)

    @transform
    class NormalizeRows(Transform):
        rows = input(Row)
        normalized = output(Row)

        @step(output=normalized)
        def normalize(self, row: Row) -> Row:
            return Row(id=row.id)

        @raw(target_platform="spark-connect")
        def clean(self, *, normalized, spark, ctx):
            return normalized

    plan = _analysis(NormalizeRows)

    hook = plan.steps[0].after_hooks[0]
    assert hook.name == "clean"
    assert hook.phase == "raw"
    assert hook.lanes[0].name == "normalized"
    assert hook.outputs[0].name == "normalized"
    assert hook.target_platform == "spark-connect"


def test_raw_before_the_first_step_replaces_its_source_lane() -> None:
    """A leading raw method runs before the first source-ordered step."""

    class Row(Schema):
        id = string(nullable=False)

    @transform
    class NormalizeRows(Transform):
        rows = input(Row)
        normalized = output(Row)

        @raw(inout=input(rows) | lane(rows))
        def prepare(self, *, rows, spark, ctx):
            return rows

        @step(output=normalized)
        def normalize(self, row: Row) -> Row:
            return Row(id=row.id)

    plan = _analysis(NormalizeRows)

    hook = plan.steps[0].before_hooks[0]
    assert (hook.name, hook.phase, hook.lanes[0].name, hook.outputs[0].name) == ("prepare", "raw", "rows", "rows")


def test_raw_pipe_binds_original_input_and_materialized_output() -> None:
    """I can select an original input and a current output without an inputs namespace."""

    class Row(Schema):
        id = string(nullable=False)

    @transform
    class PublishRows(Transform):
        rows = input(Row)
        published = output(Row)

        @step(output=published)
        def publish(self, row: Row) -> Row:
            return Row(id=row.id)

        @raw(inout=input(rows) | output(published))
        def restore(self, *, rows, published, spark, ctx):
            return published

    hook = _analysis(PublishRows).steps[0].after_hooks[0]

    assert tuple(lane.name for lane in hook.lanes) == ("rows", "published")
    assert hook.sources == ("input:rows", "published")
    assert tuple(output.name for output in hook.outputs) == ("published",)


def test_raw_pipe_rejects_an_unmaterialized_output_parameter() -> None:
    """I get a useful error when an output argument has not been produced yet."""

    class Row(Schema):
        id = string(nullable=False)

    @transform
    class PublishRows(Transform):
        rows = input(Row)
        published = output(Row)

        def normalize(self, row: Row) -> Row:
            return Row(id=row.id)

        @raw(inout=lane(rows) | output(published))
        def publish(self, *, rows, published, spark, ctx):
            return published

    with pytest.raises(StructureCompileError, match="published is not available"):
        _analysis(PublishRows)


def test_before_and_after_are_retired_from_public_namespaces() -> None:
    """@raw is the only hook-defining decorator."""

    assert not hasattr(structure, "before")
    assert not hasattr(structure, "after")
