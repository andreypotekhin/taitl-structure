from __future__ import annotations

from typing import cast

import pytest

from structure import Schema, Transform, input, output, transform
from structure.core.cli.commands.RenderExplainReport import render_explain_report
from structure.core.compiler.api import Compiler
from structure.core.compiler.compileability.streaming_compatibility.api import StreamingSupport
from structure.plugin.pyspark import (
    array,
    explode_array,
    explode_map,
    explode_outer_array,
    explode_outer_map,
    long,
    map,
    posexplode_array,
    posexplode_map,
    posexplode_outer_array,
    posexplode_outer_map,
    string,
)
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.render.commands.RenderPySparkStep import render_pyspark_step


class ArrayDocument(Schema):
    values = array(string(), contains_null=False, nullable=False)


class OuterArrayDocument(Schema):
    values = array(string(), contains_null=True, nullable=True)


class MapDocument(Schema):
    attributes = map(string(), string(), value_contains_null=True, nullable=False)


class OuterMapDocument(Schema):
    attributes = map(string(), string(), value_contains_null=True, nullable=True)


class ScalarValue(Schema):
    value = string(nullable=False)


class OuterScalarValue(Schema):
    value = string(nullable=True)


class PositionedScalarValue(Schema):
    ordinal = long(nullable=False)
    value = string(nullable=False)


class OuterPositionedScalarValue(Schema):
    ordinal = long(nullable=True)
    value = string(nullable=True)


class MapValue(Schema):
    key = string(nullable=False)
    value = string(nullable=True)


class OuterMapValue(Schema):
    key = string(nullable=True)
    value = string(nullable=True)


class PositionedMapValue(Schema):
    ordinal = long(nullable=False)
    key = string(nullable=False)
    value = string(nullable=True)


class OuterPositionedMapValue(Schema):
    ordinal = long(nullable=True)
    key = string(nullable=True)
    value = string(nullable=True)


class CollisionArrayDocument(Schema):
    value = string(nullable=False)
    values = array(string(), contains_null=False, nullable=False)


class CollisionMapDocument(Schema):
    key = string(nullable=False)
    attributes = map(string(), string(), value_contains_null=True, nullable=False)


@transform
class ExplodeArrayParity(Transform):
    documents = input(ArrayDocument)
    values = output(ScalarValue)

    def expand(self, document: ArrayDocument) -> ScalarValue:
        value = explode_array(document.values, as_=ScalarValue, value_field="value", scope="value")
        return ScalarValue(value=value.value)


@transform
class ExplodeOuterArrayParity(Transform):
    documents = input(OuterArrayDocument)
    values = output(OuterScalarValue)

    def expand(self, document: OuterArrayDocument) -> OuterScalarValue:
        value = explode_outer_array(document.values, as_=OuterScalarValue, value_field="value", scope="value")
        return OuterScalarValue(value=value.value)


@transform
class PosexplodeArrayParity(Transform):
    documents = input(ArrayDocument)
    values = output(PositionedScalarValue)

    def expand(self, document: ArrayDocument) -> PositionedScalarValue:
        value = posexplode_array(document.values, as_=PositionedScalarValue, value_field="value", scope="value")
        return PositionedScalarValue(ordinal=value.ordinal, value=value.value)


@transform
class PosexplodeOuterArrayParity(Transform):
    documents = input(OuterArrayDocument)
    values = output(OuterPositionedScalarValue)

    def expand(self, document: OuterArrayDocument) -> OuterPositionedScalarValue:
        value = posexplode_outer_array(
            document.values,
            as_=OuterPositionedScalarValue,
            value_field="value",
            scope="value",
        )
        return OuterPositionedScalarValue(ordinal=value.ordinal, value=value.value)


@transform
class ExplodeMapParity(Transform):
    documents = input(MapDocument)
    values = output(MapValue)

    def expand(self, document: MapDocument) -> MapValue:
        value = explode_map(
            document.attributes,
            as_=MapValue,
            key_field="key",
            value_field="value",
            scope="attribute",
        )
        return MapValue(key=value.key, value=value.value)


@transform
class ExplodeOuterMapParity(Transform):
    documents = input(OuterMapDocument)
    values = output(OuterMapValue)

    def expand(self, document: OuterMapDocument) -> OuterMapValue:
        value = explode_outer_map(
            document.attributes,
            as_=OuterMapValue,
            key_field="key",
            value_field="value",
            scope="attribute",
        )
        return OuterMapValue(key=value.key, value=value.value)


@transform
class PosexplodeMapParity(Transform):
    documents = input(MapDocument)
    values = output(PositionedMapValue)

    def expand(self, document: MapDocument) -> PositionedMapValue:
        value = posexplode_map(
            document.attributes,
            as_=PositionedMapValue,
            key_field="key",
            value_field="value",
            scope="attribute",
        )
        return PositionedMapValue(ordinal=value.ordinal, key=value.key, value=value.value)


@transform
class PosexplodeOuterMapParity(Transform):
    documents = input(OuterMapDocument)
    values = output(OuterPositionedMapValue)

    def expand(self, document: OuterMapDocument) -> OuterPositionedMapValue:
        value = posexplode_outer_map(
            document.attributes,
            as_=OuterPositionedMapValue,
            key_field="key",
            value_field="value",
            scope="attribute",
        )
        return OuterPositionedMapValue(ordinal=value.ordinal, key=value.key, value=value.value)


@pytest.mark.parametrize(
    ("transform_type", "operation", "source", "detail"),
    (
        (ExplodeArrayParity, "explode_array", "documents.values", {"value_field": "value", "ordinal": None}),
        (
            ExplodeOuterArrayParity,
            "explode_outer_array",
            "documents.values",
            {"value_field": "value", "ordinal": None},
        ),
        (PosexplodeArrayParity, "posexplode_array", "documents.values", {"value_field": "value", "ordinal": "ordinal"}),
        (
            PosexplodeOuterArrayParity,
            "posexplode_outer_array",
            "documents.values",
            {"value_field": "value", "ordinal": "ordinal"},
        ),
        (ExplodeMapParity, "explode_map", "documents.attributes", {"key_field": "key", "value_field": "value", "ordinal": None}),
        (
            ExplodeOuterMapParity,
            "explode_outer_map",
            "documents.attributes",
            {"key_field": "key", "value_field": "value", "ordinal": None},
        ),
        (PosexplodeMapParity, "posexplode_map", "documents.attributes", {"key_field": "key", "value_field": "value", "ordinal": "ordinal"}),
        (
            PosexplodeOuterMapParity,
            "posexplode_outer_map",
            "documents.attributes",
            {"key_field": "key", "value_field": "value", "ordinal": "ordinal"},
        ),
    ),
)
def test_v7_generator_matrix_keeps_lowering_explain_traceability_and_streaming_parity(
    transform_type: type[Transform],
    operation: str,
    source: str,
    detail: dict[str, str | None],
) -> None:
    lowered = cast(PySparkExecutionPlan, Compiler.frontend.compile()(transform_type, materialize_schemas=False).lowered)
    recipe = lowered.steps[0].operations[0]

    assert recipe.kind == operation
    assert recipe.scalar_generator is not None or recipe.map_generator is not None
    generator = recipe.scalar_generator or recipe.map_generator
    assert generator is not None
    for name, expected in detail.items():
        assert getattr(generator, name) == expected

    rendered = render_pyspark_step(lowered.steps[0], current="documents", sources={"documents": "documents"})
    assert f"F.{generator.function}(" in rendered
    assert "F.col(" in rendered

    explanation = render_explain_report(transform_type)
    assert f"{operation}(row_multiplying" in explanation
    report = Compiler.compileability.streaming()(lowered, required=True)
    assert report.support is StreamingSupport.COMPATIBLE
    assert report.findings == ()

    traceability = Compiler.traceability.build()(
        lowered,
        source_transform=f"tests.{transform_type.__name__}",
        transform_module="tests.generated",
    )
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}
    dependency = dependencies[f"expand.{operation}[0].{generator.scope}"]
    assert dependency.sources == (source,)
    assert dependency.operation == operation
    for name, expected in detail.items():
        assert dependency.detail[name] == expected


def test_v7_generator_matrix_preserves_nullability_and_collection_flags() -> None:
    assert ArrayDocument._structure_fields["values"].nullable is False
    assert ArrayDocument._structure_fields["values"].type.contains_null is False
    assert OuterArrayDocument._structure_fields["values"].nullable is True
    assert OuterArrayDocument._structure_fields["values"].type.contains_null is True
    assert MapDocument._structure_fields["attributes"].nullable is False
    assert MapDocument._structure_fields["attributes"].type.value_contains_null is True
    assert OuterMapDocument._structure_fields["attributes"].nullable is True

    for schema in (OuterScalarValue, OuterPositionedScalarValue, OuterMapValue, OuterPositionedMapValue):
        assert all(field.nullable for field in schema._structure_fields.values())
    assert MapValue._structure_fields["key"].nullable is False
    assert MapValue._structure_fields["value"].nullable is True
    assert PositionedMapValue._structure_fields["ordinal"].nullable is False


def test_v7_generator_matrix_rejects_source_column_collisions_before_execution() -> None:
    class BadArrayTransform(Transform):
        documents = input(CollisionArrayDocument)
        values = output(ScalarValue)

        def expand(self, document: CollisionArrayDocument) -> ScalarValue:
            value = explode_array(document.values, as_=ScalarValue, value_field="value")
            return ScalarValue(value=value.value)

    class BadMapTransform(Transform):
        documents = input(CollisionMapDocument)
        values = output(MapValue)

        def expand(self, document: CollisionMapDocument) -> MapValue:
            value = explode_map(
                document.attributes,
                as_=MapValue,
                key_field="key",
                value_field="value",
            )
            return MapValue(key=value.key, value=value.value)

    for transform_type in (BadArrayTransform, BadMapTransform):
        with pytest.raises(TypeError, match="generated columns collide with current input"):
            Compiler.frontend.compile()(transform_type, materialize_schemas=False)
