from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import array, long, posexplode_array, string
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class Document(Schema):
    values = array(string(), contains_null=False, nullable=False)


class ExpandedValue(Schema):
    ordinal = long(nullable=False)
    value = string(nullable=False)


class Result(Schema):
    value = string(nullable=False)
    ordinal = long(nullable=False)


class ExpandValues(Transform):
    documents = input(Document)
    values = output(Result)

    def expand(self, document: Document) -> Result:
        value = posexplode_array(document.values, as_=ExpandedValue, value_field="value", scope="value")
        return Result(value=value.value, ordinal=value.ordinal)


def test_posexplode_array_is_typed_and_optimizer_visible() -> None:
    operation = _lowered().steps[0].operations[0]

    assert operation.kind == "posexplode_array"
    assert operation.scalar_generator is not None
    assert operation.scalar_generator.value_field == "value"
    assert operation.scalar_generator.ordinal == "ordinal"
    rendered = render_pyspark_step(_lowered().steps[0], current="documents", sources={"documents": "documents"})
    assert "F.posexplode(F.col(\"document.values\"))" in rendered


def test_posexplode_array_rejects_struct_arrays() -> None:
    class BadDocument(Schema):
        values = array(array(string(), contains_null=False), contains_null=False, nullable=False)

    class BadTransform(Transform):
        documents = input(BadDocument)
        values = output(Result)

        def expand(self, document: BadDocument) -> Result:
            value = posexplode_array(document.values, as_=ExpandedValue, value_field="value")
            return Result(value=value.value, ordinal=value.ordinal)

    with pytest.raises(TypeError, match="primitive scalar"):
        Compiler.frontend.compile()(BadTransform, materialize_schemas=False)


def _lowered() -> PySparkExecutionPlan:
    return cast(PySparkExecutionPlan, Compiler.frontend.compile()(ExpandValues, materialize_schemas=False).lowered)
