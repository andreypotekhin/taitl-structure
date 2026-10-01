from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import json_tuple, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.types import StringType
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class Document(Schema):
    payload = string(nullable=True)


class CollisionDocument(Schema):
    payload = string(nullable=True)
    customer_id = string(nullable=True)


class Extracted(Schema):
    customer_id = string(nullable=True)
    tier = string(nullable=True)


class Published(Schema):
    customer_id = string(nullable=True)
    tier = string(nullable=True)


class ExtractJsonTuple(Transform):
    documents = input(Document)
    published = output(Published)

    def publish(self, document: Document) -> Published:
        extracted = json_tuple(
            document.payload,
            as_=Extracted,
            fields={"customer_id": "customerId"},
            scope="payload_fields",
        )
        return Published(customer_id=extracted.customer_id, tier=extracted.tier)


def _lowered() -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(ExtractJsonTuple, materialize_schemas=False).lowered)


def test_json_tuple_is_a_typed_row_preserving_operation() -> None:
    operation = _lowered().steps[0].operations[0]

    assert operation.kind == "json_tuple"
    assert operation.json_tuple is not None
    assert operation.json_tuple.fields == (("customer_id", "customerId"), ("tier", "tier"))
    assert all(
        field.type.name == StringType().name and field.nullable for field in Extracted._structure_fields.values()
    )


def test_json_tuple_renders_native_function_and_output_aliases() -> None:
    step = _lowered().steps[0]
    rendered = render_pyspark_step(step, current="document", sources={"document": "document"})

    assert (
        'F.json_tuple(F.col("document.payload"), \'customerId\', \'tier\').alias(\'customer_id\', \'tier\')' in rendered
    )


@pytest.mark.parametrize(
    ("schema", "message"),
    [
        (type("Required", (Schema,), {"value": string(nullable=False)}), "nullable String"),
        (type("Empty", (Schema,), {}), "at least one output field"),
    ],
)
def test_json_tuple_rejects_undeclared_output_shapes(schema: type[Schema], message: str) -> None:
    class BadTransform(Transform):
        documents = input(Document)
        published = output(Published)

        def publish(self, document: Document) -> Published:
            extracted = json_tuple(document.payload, as_=schema)
            return Published(customer_id=extracted.value, tier=extracted.value)

    with pytest.raises(TypeError, match=message):
        Compiler.frontend.compile()(BadTransform, materialize_schemas=False)


def test_json_tuple_rejects_generated_column_collisions() -> None:
    class BadTransform(Transform):
        documents = input(CollisionDocument)
        published = output(Published)

        def publish(self, document: CollisionDocument) -> Published:
            extracted = json_tuple(document.payload, as_=Extracted)
            return Published(customer_id=extracted.customer_id, tier=extracted.tier)

    with pytest.raises(TypeError, match="generated columns collide with current input"):
        Compiler.frontend.compile()(BadTransform, materialize_schemas=False)


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"unknown": "member"}, "undeclared output field"),
        ({"customer_id": 1}, "member names must be strings"),
        (["customer_id", "member"], "must be a mapping"),
    ],
)
def test_json_tuple_rejects_invalid_member_mappings(fields: object, message: str) -> None:
    class BadTransform(Transform):
        documents = input(Document)
        published = output(Published)

        def publish(self, document: Document) -> Published:
            extracted = json_tuple(
                document.payload,
                as_=Extracted,
                fields=cast(Mapping[str, str], fields),
            )
            return Published(customer_id=extracted.customer_id, tier=extracted.tier)

    with pytest.raises(TypeError, match=message):
        Compiler.frontend.compile()(BadTransform, materialize_schemas=False)
