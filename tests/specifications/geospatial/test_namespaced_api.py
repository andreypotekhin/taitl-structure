from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest

from structure import Schema
from structure.plugin.api.v1.model import CapabilityRequirement
from structure.plugin.pyspark import databricks, geography, geometry, sedona, st_geomfromwkb
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.dsl.types import GeographyType, GeometryType, SedonaGeometryType, StringType
from structure.plugin.pyspark.geo.runtime import call_geospatial_function
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import RenderPySparkExpression
from structure.plugin.pyspark.schema.commands.RenderPySparkSchemaModule import render_pyspark_schema_module
from structure.plugin.pyspark.schema.runtime import native_spatial_type
from structure.plugin.pyspark.schema.SketchMetadata import field_metadata


def test_spatial_types_accept_mixed_and_fixed_srid_but_reject_booleans() -> None:
    assert geometry().type == GeometryType(None)
    assert geography(srid=4326).type == GeographyType(4326)
    assert sedona.geometry(srid=4326).type == SedonaGeometryType(4326)
    with pytest.raises(TypeError, match="integer or None"):
        geometry(srid=True)


def test_namespaced_helpers_preserve_explicit_provider_and_signature() -> None:
    native = st_geomfromwkb(b"wkb")
    dbx = databricks.st_geomfromwkb(b"wkb", 4326)
    external = sedona.st_geomfromwkt("POINT (1 2)", 4326)

    assert native.type == GeometryType(0)
    assert dbx.type == GeometryType(4326)
    assert external.type == SedonaGeometryType(4326)
    assert (native.data["provider"], native.data["operation"]) == ("apache", "st_geomfromwkb")
    assert len(native.args) == 1
    assert dbx.data["provider"] == "databricks"
    assert external.data["provider"] == "sedona"


def test_sedona_predicate_requires_same_representation_and_fixed_srid() -> None:
    first = sedona.st_geomfromwkt("POINT (0 0)", 4326)
    second = sedona.st_geomfromwkt("POINT (1 1)", 4326)
    assert sedona.st_intersects(first, second).type.name == "boolean"
    with pytest.raises(TypeError, match="same fixed SRID"):
        sedona.st_intersects(sedona.st_geomfromwkt("POINT (0 0)"), second)
    with pytest.raises(TypeError, match="two spatial expressions"):
        sedona.st_intersects(sedona.geometry(4326), geometry(4326))


def test_spatial_values_require_explicit_binary_helpers_for_conversion() -> None:
    shape = sedona.st_geomfromwkt("POINT (0 0)", 4326)
    with pytest.raises(TypeError, match="does not cast spatial values.*Binary"):
        shape.cast(StringType())


def test_geospatial_helper_is_capability_gated_by_namespace() -> None:
    expression = st_geomfromwkb(b"payload")
    capabilities = PySparkCapabilities(target_profile=">=4.1,<4.2")
    recipe = MapPySparkExpression().map(expression, capabilities=capabilities)
    source = RenderPySparkExpression()(recipe)

    assert "call_geospatial_function('apache', 'st_geomfromwkb'" in source
    decision = PySparkCapabilities(target_profile=">=4.0,<4.1").supports(
        CapabilityRequirement(group="geospatial", name="apache.st_geomfromwkb")
    )
    assert not decision.supported


def test_native_schema_type_is_not_admitted_on_default_profile() -> None:
    decision = PySparkCapabilities().supports(
        CapabilityRequirement(group="schema", name="native_geometry")
    )
    assert not decision.supported
    assert "exact Spark 4.0 or 4.1" in decision.why


def test_native_schema_requires_runtime_constructor_and_keeps_srid() -> None:
    class FakeTypes:
        @staticmethod
        def GeometryType(srid): return ("geometry", srid)
        @staticmethod
        def GeographyType(srid): return ("geography", srid)

    from structure.plugin.pyspark.schema.commands.MaterializePySparkSchema import materialize_pyspark_schema

    assert materialize_pyspark_schema.type(GeometryType(None), types=FakeTypes) == ("geometry", "ANY")
    assert materialize_pyspark_schema.type(GeographyType(4326), types=FakeTypes) == ("geography", 4326)
    with pytest.raises(RuntimeError, match="native spatial type constructors.*Geospatial.ref.md"):
        materialize_pyspark_schema.type(GeometryType(4326), types=SimpleNamespace())
    with pytest.raises(RuntimeError, match="native spatial type constructors.*Geospatial.ref.md"):
        native_spatial_type(SimpleNamespace(), "GeometryType", 4326)


def test_generated_schema_uses_native_guard_and_sedona_type_metadata() -> None:
    class SpatialRow(Schema):
        native = geometry(srid=4326)
        mixed = geography()
        external = sedona.geometry(srid=3857)

    source = render_pyspark_schema_module((SpatialRow,))
    assert "native_spatial_type(T, 'GeometryType', 4326)" in source
    assert "native_spatial_type(T, 'GeographyType', None)" in source
    assert "sedona_geometry_type(3857)" in source
    assert '"kind": "sedona_geometry"' in source
    assert "from structure.plugin.pyspark.schema.runtime import sedona_geometry_type" in source


def test_schema_reader_reconstructs_native_and_sedona_spatial_types() -> None:
    from structure.plugin.pyspark.schema.logic.MapPySparkSchemaToStructureSource import (
        MapPySparkSchemaToStructureSource,
    )

    SparkGeometry = type("GeometryType", (), {"__module__": "pyspark.sql.types"})
    SparkGeography = type("GeographyType", (), {"__module__": "pyspark.sql.types"})
    SedonaGeometry = type("GeometryType", (), {"__module__": "sedona.spark.sql.types"})
    schema = type("StructType", (), {})()
    schema.fields = [
        SimpleNamespace(name="native", dataType=SparkGeometry(), nullable=True, metadata={}),
        SimpleNamespace(name="earth", dataType=SparkGeography(), nullable=True, metadata={}),
        SimpleNamespace(
            name="external",
            dataType=SedonaGeometry(),
            nullable=True,
            metadata={"structure.logical_type": {"version": 1, "kind": "sedona_geometry", "srid": 4326}},
        ),
    ]
    mapped = MapPySparkSchemaToStructureSource()(schema, to="Spatial")
    fields = mapped.classes[0].fields
    assert [field.type for field in fields] == [
        "geometry(srid=None)", "geography(srid=None)", "sedona.geometry(srid=4326)"
    ]


def test_sedona_metadata_is_recursive_and_conflicts_are_rejected() -> None:
    from structure.plugin.pyspark.dsl.types import array

    expected = {"version": 1, "kind": "array", "element": {"version": 1, "kind": "sedona_geometry", "srid": 4326}}
    assert field_metadata(array(SedonaGeometryType(4326))) == {"structure.logical_type": expected}
    with pytest.raises(ValueError, match="conflicting structure.logical_type"):
        field_metadata(SedonaGeometryType(3857), {"structure.logical_type": expected})


def test_runtime_dispatch_never_falls_back_to_another_provider(monkeypatch) -> None:
    selected = ModuleType("pyspark.databricks.sql.functions")
    setattr(selected, "st_srid", lambda value: ("databricks", value))
    monkeypatch.setitem(sys.modules, selected.__name__, selected)

    assert call_geospatial_function("databricks", "st_srid", "column") == ("databricks", "column")
    with pytest.raises(RuntimeError, match="apache.st_srid.*unavailable"):
        call_geospatial_function("apache", "st_srid", "column")


def test_sedona_dispatch_uses_its_public_function_modules(monkeypatch) -> None:
    from structure.plugin.pyspark.geo import runtime

    seen = []

    def import_public_module(name):
        seen.append(name)
        return SimpleNamespace(ST_GeomFromWKT=lambda value, srid: (value, srid))

    monkeypatch.setattr(runtime, "import_module", import_public_module)
    assert call_geospatial_function("sedona", "st_geomfromwkt", "POINT (0 0)", 4326) == (
        "POINT (0 0)", 4326
    )
    assert seen == ["sedona.spark.sql.st_constructors"]
