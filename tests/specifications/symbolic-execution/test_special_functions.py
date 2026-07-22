from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import *
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def _compile(transform, *, warn_on_udfs: bool = True):
    return Compiler.frontend.compile()(
        transform,
        materialize_schemas=False,
        warn_on_udfs=warn_on_udfs,
    )


class Raw(Schema):
    id = string(nullable=False)


class Published(Schema):
    id = string(nullable=False)


def test_special_expr_helper_call_through_self_compiles_transparently() -> None:
    class Publish(Transform):
        rows = input(Raw)
        published = output(Published)

        @special(type="expr")
        def clean(value):
            return value

        def publish(self, row: Raw) -> Published:
            return Published(id=self.clean(row.id))

    expression = cast(PySparkStepBody, _compile(Publish).analysis.steps[0].plugin_body).projection[0].expression

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

    plan = _compile(Publish).analysis

    assert cast(PySparkStepBody, plan.steps[0].plugin_body).projection[0].expression.kind == "python_udf"
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

    plan = _compile(Publish, warn_on_udfs=False).analysis

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
        _compile(Publish)

    assert raised.value.diagnostic.code == "DSL-E0401"
    assert "return_type" in raised.value.diagnostic.problem_text()


@pytest.mark.parametrize("nullable", [1, "true", None])
def test_special_udf_requires_a_boolean_nullable_declaration(nullable: object) -> None:
    with pytest.raises(TypeError, match=r'@special\(type="udf"\) nullable must be a Boolean'):
        special(type="udf", nullable=nullable)(lambda value: value)


@pytest.mark.parametrize("option", ["streaming", "validate_intermediate"])
@pytest.mark.parametrize("value", [1, "true", None])
def test_transform_requires_boolean_class_options(option: str, value: object) -> None:
    with pytest.raises(TypeError, match=rf"{option} must be a Boolean"):
        transform(**{option: value})(type("InvalidOptions", (Transform,), {}))


def test_transform_rejects_the_replaced_streaming_option() -> None:
    with pytest.raises(TypeError, match="unknown class option.*streaming_compatible"):
        transform(streaming_compatible=True)(type("InvalidOptions", (Transform,), {}))


@pytest.mark.parametrize("value", [1, "true", None])
def test_input_requires_a_boolean_streaming_declaration(value: object) -> None:
    with pytest.raises(TypeError, match=r"input\(streaming=\.\.\.\) must be a Boolean"):
        input(Raw, streaming=cast(Any, value))


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

    recipe = cast(PySparkExecutionPlan, _compile(Publish).lowered)
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

    recipe = cast(PySparkExecutionPlan, _compile(Publish).lowered)
    traceability = Compiler.traceability.build()(
        recipe,
        source_transform=f"{Publish.__module__}.{Publish.__name__}",
        transform_module=f"{Publish.__module__}.{Publish.__name__}Generated",
    )

    assert ("publish", "clean", "expression", "python UDF body") in {
        (boundary.step, boundary.hook, boundary.phase, boundary.reason) for boundary in traceability.opaque_boundaries
    }
