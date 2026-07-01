import pytest

from structure import (
    String,
    Structure,
    StructureCompileError,
    Transform,
    after,
    before,
    field,
    input,
    output,
    transform,
)
from structure.app.dsl.api import SchemaMode, compile_transform


def test_hooks_attach_to_declared_subtransform_boundaries(orders_recipe) -> None:
    """I can attach a hook to a subtransform using @before(method, lane=lane) or @after(method, lane=lane)."""

    assert [hook.name for hook in orders_recipe.steps[0].before_hooks] == ["use_current_orders"]
    assert [hook.name for hook in orders_recipe.steps[0].after_hooks] == ["remove_negative_totals"]
    assert [hook.name for hook in orders_recipe.steps[3].after_hooks] == ["note_lookup_inputs"]
    assert [hook.name for hook in orders_recipe.steps[4].after_hooks] == ["add_quality_columns"]


def test_hooks_record_input_access_and_projection_validation_contracts(orders_recipe) -> None:
    """I can opt a hook into original input access."""

    lookup = orders_recipe.steps[3].after_hooks[0]
    quality = orders_recipe.steps[4].after_hooks[0]

    assert lookup.pass_inputs
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


def test_hooks_record_target_backend_metadata() -> None:
    """Hooks carry v1 target_backend metadata through the PySpark recipe."""

    class Row(Structure):
        id = field(String(), nullable=False)

    @transform
    class NormalizeRows(Transform):
        rows = input(Row)
        normalized = output(Row)

        def normalize(self, row: Row) -> Row:
            return Row(id=row.id)

        @before(normalize, lane=rows, target_backend=["pyspark"])
        def prepare(self, *, rows, spark, ctx):
            return rows

        @after(normalize, lane=rows, target_backend="pyspark")
        def clean(self, *, rows, spark, ctx):
            return rows

    plan = compile_transform(NormalizeRows)

    assert plan.steps[0].before_hooks[0].target_backend == ("pyspark",)
    assert not plan.steps[0].before_hooks[0].target_defaulted
    assert plan.steps[0].after_hooks[0].target_backend == ("pyspark",)
    assert not plan.steps[0].after_hooks[0].target_defaulted


def test_non_pyspark_only_hook_target_fails_before_runtime() -> None:
    """V1 accepts hook target syntax, but active execution is still PySpark only."""

    class Row(Structure):
        id = field(String(), nullable=False)

    @transform
    class NormalizeRows(Transform):
        rows = input(Row)
        normalized = output(Row)

        def normalize(self, row: Row) -> Row:
            return Row(id=row.id)

        @after(normalize, lane=rows, target_backend="polars")
        def clean(self, *, rows, spark, ctx):
            return rows

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(NormalizeRows)

    assert "targets polars" in str(raised.value)
    assert "PySpark only" in str(raised.value)


def test_generated_code_calls_source_transform_hooks_directly(orders_transform_text) -> None:
    """Generated code calls hooks directly on the source transform instance."""

    assert "        self._impl = EnrichOrders()" in orders_transform_text
    assert "        orders = self._impl.use_current_orders(" in orders_transform_text
    assert "        orders = self._impl.remove_negative_totals(" in orders_transform_text
    assert "        orders = self._impl.note_lookup_inputs(" in orders_transform_text
    assert "        published = self._impl.add_quality_columns(" in orders_transform_text
