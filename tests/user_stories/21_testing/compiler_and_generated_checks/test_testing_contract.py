import ast

import pytest

from structure import String, Structure, StructureCompileError, Transform, field, input, output, transform
from structure.app.dsl.api import compile_transform


def test_user_story_tests_compile_transforms_before_deployment(orders_plan) -> None:
    """I can compile transforms during tests."""

    assert orders_plan.name == "EnrichOrders"
    assert [step.name for step in orders_plan.steps] == [
        "normalize",
        "add_customer",
        "add_product",
        "add_promotion",
        "publish",
    ]


def test_generated_code_can_be_syntax_checked_without_spark_runtime(orders_transform_text) -> None:
    """I can snapshot generated code so generator changes are reviewable."""

    ast.parse(orders_transform_text)
    assert "class EnrichOrdersGenerated:" in orders_transform_text
    assert "return TransformResult" in orders_transform_text


def test_intentionally_broken_transform_tests_keep_diagnostics_actionable() -> None:
    """I can run intentionally broken transform tests so compiler diagnostics stay actionable."""

    class Raw(Structure):
        id = field(String(), nullable=False)

    class Published(Structure):
        id = field(String(), nullable=False)
        status = field(String(), nullable=False)

    @transform
    class MissingOutputField(Transform):
        rows = input(Raw)
        published = output(Published)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(MissingOutputField)

    assert raised.value.diagnostic.code == "DSL-E0402"
    assert raised.value.diagnostic.context == {"field": "status", "schema": "Published"}
