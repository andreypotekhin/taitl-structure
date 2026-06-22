import pytest

from structure import String, Structure, StructureCompileError, Transform, field, input, transform
from structure.app.dsl.api import compile_transform


class Raw(Structure):
    id = field(String(), nullable=False)


class Clean(Structure):
    id = field(String(), nullable=False)


class Published(Structure):
    id = field(String(), nullable=False)
    status = field(String(), nullable=False)


def test_v1_unsupported_python_boolean_expression_reports_dsl_diagnostic() -> None:
    @transform
    class BadBoolean(Transform):
        rows = input(Raw)

        def normalize(self, row: Raw) -> Clean:
            if row.id:
                return Clean(id=row.id)
            return Clean(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadBoolean)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0401"
    assert diagnostic.docs == "docs/Diagnostics.md#dsl-e0401"
    assert diagnostic.source.endswith("BadBoolean.normalize")
    assert "unsupported symbolic code" in diagnostic.problem_text()
    assert "Structure expression helpers" in diagnostic.use_text()


def test_v1_schema_flow_mismatch_reports_transform_structure_diagnostic() -> None:
    @transform
    class BadFlow(Transform):
        rows = input(Raw)

        def normalize(self, row: Raw) -> Clean:
            return Clean(id=row.id)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id, status="ready")

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(BadFlow)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0402"
    assert diagnostic.context == {"expected": "Raw", "actual": "Clean"}
    assert diagnostic.source.endswith("BadFlow.publish")
    assert "previous subtransform returns Clean" in diagnostic.problem_text()


def test_v1_missing_output_field_reports_transform_structure_diagnostic() -> None:
    @transform
    class MissingOutput(Transform):
        rows = input(Raw)

        def publish(self, row: Raw) -> Published:
            return Published(id=row.id)

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(MissingOutput)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0402"
    assert diagnostic.context == {"field": "status", "schema": "Published"}
    assert diagnostic.source.endswith("MissingOutput.publish")
    assert "Published.status is not assigned" in diagnostic.problem_text()
