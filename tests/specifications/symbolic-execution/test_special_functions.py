from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.platform.pyspark.api import PySpark


class Raw(Schema):
    id = field.string(nullable=False)


class Published(Schema):
    id = field.string(nullable=False)


def test_special_expr_helper_call_through_self_compiles_transparently() -> None:
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        @special(type="expr")
        def clean(value):
            return value

        def publish(self, row: Raw) -> Published:
            return Published(id=self.clean(row.id))

    expression = compile_transform(Publish).steps[0].projection[0].expression

    assert expression.kind == "field"


def test_special_udf_records_optimizer_warning_by_default() -> None:
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        @special(type="udf", return_type=types.string(), nullable=False)
        def clean(value: Any):
            return value.strip()

        def publish(self, row: Raw) -> Published:
            return Published(id=self.clean(row.id))

    plan = compile_transform(Publish)

    assert plan.steps[0].projection[0].expression.kind == "python_udf"
    assert [diagnostic.code for diagnostic in plan.diagnostics] == ["DSL-W0403"]


def test_special_udf_warning_can_be_disabled_by_compiler_config() -> None:
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        @special(type="udf", return_type=types.string(), nullable=False)
        def clean(value: Any):
            return value.strip()

        def publish(self, row: Raw) -> Published:
            return Published(id=self.clean(row.id))

    plan = compile_transform(Publish, warn_on_udfs=False)

    assert plan.diagnostics == ()


def test_special_udf_requires_return_type_or_supported_annotation() -> None:
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        @special(type="udf")
        def clean(value: Any):
            return value.strip()

        def publish(self, row: Raw) -> Published:
            return Published(id=self.clean(row.id))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "return_type" in raised.value.diagnostic.problem_text()


@pytest.mark.parametrize("nullable", [1, "true", None])
def test_special_udf_requires_a_boolean_nullable_declaration(nullable: object) -> None:
    with pytest.raises(TypeError, match=r'@special\(type="udf"\) nullable must be a Boolean'):
        special(type="udf", nullable=nullable)(lambda value: value)


@pytest.mark.parametrize("option", ["streaming_compatible", "validate_intermediate"])
@pytest.mark.parametrize("value", [1, "true", None])
def test_transform_requires_boolean_class_options(option: str, value: object) -> None:
    with pytest.raises(TypeError, match=rf"{option} must be a Boolean"):
        transform(**{option: value})(type("InvalidOptions", (Transform,), {}))


@pytest.mark.parametrize("option", ["project_output", "streaming_safe"])
@pytest.mark.parametrize("value", [1, "true", None])
def test_raw_requires_boolean_options(option: str, value: object) -> None:
    with pytest.raises(TypeError, match=rf"@raw\({option}=\.\.\.\) requires a Boolean"):
        raw(**cast(Any, {option: value}))(lambda: None)


def test_raw_requires_a_schema_mode() -> None:
    with pytest.raises(TypeError, match=r"@raw\(schema_mode=\.\.\.\) requires a SchemaMode value"):
        raw(schema_mode=cast(Any, "strict"))(lambda: None)


def test_special_udf_renders_generated_pyspark_udf_call() -> None:
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        @special(type="udf", return_type=types.string(), nullable=False)
        def clean(value: Any):
            return value.strip()

        def publish(self, row: Raw) -> Published:
            return Published(id=self.clean(row.id))

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    expression = recipe.steps[0].projection[0].expression
    text = PySpark.render.expression()(expression, scope_aliases={"rows": "rows"})

    assert text.startswith("self._structure_udf_")
    assert 'F.col("rows.id")' in text


def test_special_udf_traceability_marks_python_body_opaque() -> None:
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        @special(type="udf", return_type=types.string(), nullable=False)
        def clean(value: Any):
            return value.strip()

        def publish(self, row: Raw) -> Published:
            return Published(id=self.clean(row.id))

    recipe = PySpark.plan.lower()(compile_transform(Publish))
    traceability = Compiler.traceability.build()(
        recipe,
        source_transform=f"{Publish.__module__}.{Publish.__name__}",
        transform_module=f"{Publish.__module__}.{Publish.__name__}Generated",
    )

    assert ("publish", "clean", "expression", "python UDF body") in {
        (boundary.step, boundary.hook, boundary.phase, boundary.reason) for boundary in traceability.opaque_boundaries
    }
