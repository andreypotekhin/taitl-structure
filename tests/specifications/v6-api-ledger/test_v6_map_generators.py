from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import explode_map, long, map, posexplode_map, posexplode_outer_map, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class Document(Schema):
    attributes = map(string(), string(), value_contains_null=True, nullable=False)


class ExpandedAttribute(Schema):
    key = string(nullable=False)
    value = string(nullable=True)
    ordinal = long(nullable=False)


class Result(Schema):
    key = string(nullable=False)
    value = string(nullable=True)
    ordinal = long(nullable=False)


class OuterExpandedAttribute(Schema):
    key = string(nullable=True)
    value = string(nullable=True)
    ordinal = long(nullable=True)


class OuterResult(Schema):
    key = string(nullable=True)
    value = string(nullable=True)
    ordinal = long(nullable=True)


class ExpandAttributes(Transform):
    documents = input(Document)
    attributes = output(Result)

    def expand(self, document: Document) -> Result:
        attribute = posexplode_map(
            document.attributes,
            as_=ExpandedAttribute,
            key_field="key",
            value_field="value",
            scope="attribute",
        )
        return Result(key=attribute.key, value=attribute.value, ordinal=attribute.ordinal)


def test_posexplode_map_is_typed_and_optimizer_visible() -> None:
    operation = _lowered().steps[0].operations[0]

    assert operation.kind == "posexplode_map"
    assert operation.map_generator is not None
    assert operation.map_generator.key_field == "key"
    assert operation.map_generator.value_field == "value"
    assert operation.map_generator.ordinal == "ordinal"
    rendered = render_pyspark_step(_lowered().steps[0], current="similarity", sources={"similarity": "similarity"})
    assert 'F.posexplode(F.col("document.attributes"))' in rendered


def test_explode_map_renders_two_named_generated_fields() -> None:
    class Expand(Transform):
        documents = input(Document)
        attributes = output(Result)

        def expand(self, document: Document) -> Result:
            attribute = explode_map(
                document.attributes,
                as_=type("Expanded", (Schema,), {"key": string(nullable=False), "value": string(nullable=True)}),
                key_field="key",
                value_field="value",
                scope="attribute",
            )
            return Result(key=attribute.key, value=attribute.value, ordinal=0)

    rendered = render_pyspark_step(
        cast(PySparkExecutionPlan, Compiler.frontend.compile()(Expand, materialize_schemas=False).lowered).steps[0],
        current="similarity",
        sources={"similarity": "similarity"},
    )
    assert 'F.explode(F.col("document.attributes"))' in rendered
    assert "__structure_attribute_1_key" in rendered
    assert "__structure_attribute_1_value" in rendered


def test_posexplode_outer_map_records_outer_schema_and_traceability() -> None:
    class ExpandOuter(Transform):
        documents = input(Document)
        attributes = output(OuterResult)

        def expand(self, document: Document) -> OuterResult:
            attribute = posexplode_outer_map(
                document.attributes,
                as_=OuterExpandedAttribute,
                key_field="key",
                value_field="value",
                ordinal="ordinal",
                scope="attribute",
            )
            return OuterResult(key=attribute.key, value=attribute.value, ordinal=attribute.ordinal)

    lowered = cast(PySparkExecutionPlan, Compiler.frontend.compile()(ExpandOuter, materialize_schemas=False).lowered)
    operation = lowered.steps[0].operations[0]
    assert operation.kind == "posexplode_outer_map"
    assert operation.map_generator is not None
    assert operation.map_generator.outer is True
    text = render_explain_report(ExpandOuter)
    assert "posexplode_outer_map(row_multiplying scope=attribute schema=OuterExpandedAttribute)" in text
    traceability = Compiler.traceability.build()(
        lowered,
        source_transform="tests.ExpandOuter",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}
    dependency = dependencies["expand.posexplode_outer_map[0].attribute"]
    assert dependency.detail["key_field"] == "key"
    assert dependency.detail["value_field"] == "value"


def test_posexplode_map_rejects_nested_map_values() -> None:
    class BadDocument(Schema):
        attributes = map(string(), map(string(), string()), nullable=False)

    class BadTransform(Transform):
        documents = input(BadDocument)
        attributes = output(Result)

        def expand(self, document: BadDocument) -> Result:
            attribute = posexplode_map(
                document.attributes,
                as_=ExpandedAttribute,
                key_field="key",
                value_field="value",
            )
            return Result(key=attribute.key, value=attribute.value, ordinal=attribute.ordinal)

    with pytest.raises(TypeError, match="primitive scalar"):
        Compiler.frontend.compile()(BadTransform, materialize_schemas=False)


def _lowered() -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(ExpandAttributes, materialize_schemas=False).lowered)
