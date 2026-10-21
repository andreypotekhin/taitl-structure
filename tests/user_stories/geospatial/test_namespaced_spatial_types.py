from structure import Schema
from structure.plugin.pyspark import databricks, geometry, sedona, st_geomfromwkb
from structure.plugin.pyspark.dsl.types import GeometryType, SedonaGeometryType


def test_native_and_sedona_fields_keep_explicit_representation() -> None:
    class Shapes(Schema):
        native = geometry(srid=4326)
        external = sedona.geometry(srid=4326)

    assert Shapes._structure_fields["native"].type == GeometryType(4326)
    assert Shapes._structure_fields["external"].type == SedonaGeometryType(4326)
    assert st_geomfromwkb(b"wkb").data["provider"] == "apache"
    assert databricks.st_geomfromwkb(b"wkb").data["provider"] == "databricks"
