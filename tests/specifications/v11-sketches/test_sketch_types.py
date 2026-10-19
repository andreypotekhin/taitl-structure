from typing import cast

import pytest
from helpers.fake_pyspark_schema import ArrayType as FakeArrayType
from helpers.fake_pyspark_schema import BinaryType as FakeBinaryType
from helpers.fake_pyspark_schema import MapType as FakeMapType
from helpers.fake_pyspark_schema import StringType as FakeStringType
from helpers.fake_pyspark_schema import StructField, StructType

from structure import Schema
from structure.plugin.api.v1.model import CapabilityRequirement
from structure.plugin.pyspark import (
    bitmap,
    double,
    float,
    hll_sketch,
    kll_sketch,
    kll_sketch_agg,
    kll_sketch_get_n,
    long,
    theta_sketch,
    theta_sketch_agg,
)
from structure.plugin.pyspark.capabilities.model.PySparkCapabilities import PySparkCapabilities
from structure.plugin.pyspark.dsl.field import array as array_field
from structure.plugin.pyspark.dsl.field import map as map_field
from structure.plugin.pyspark.dsl.field import string as string_field
from structure.plugin.pyspark.dsl.types import KllSketchType, LongType, ThetaSketchType
from structure.plugin.pyspark.schema.commands.MaterializePySparkSchema import MaterializePySparkSchema
from structure.plugin.pyspark.schema.commands.RenderPySparkSchema import RenderPySparkSchema
from structure.plugin.pyspark.schema.logic.MapPySparkSchemaToStructureSource import MapPySparkSchemaToStructureSource
from structure.plugin.pyspark.schema.SketchMetadata import validate_field_metadata


class FakeTypes:
    StructField = StructField
    StructType = StructType
    ArrayType = FakeArrayType
    MapType = FakeMapType
    StringType = FakeStringType

    @staticmethod
    def BinaryType():
        return FakeBinaryType()


def test_kll_type_tracks_domain_and_uses_one_factory_contract():
    assert isinstance(kll_sketch(long()).type, KllSketchType)
    assert kll_sketch(long()).type.domain == "long"
    assert kll_sketch(float()).type.domain == "float"
    assert kll_sketch(double()).type.domain == "double"
    assert kll_sketch(long()).type != kll_sketch(double()).type

    with pytest.raises(TypeError, match=r"domain must be long\(\), float\(\), or double\(\)"):
        kll_sketch()
    with pytest.raises(TypeError, match="profile=.*removed"):
        kll_sketch(long(), profile="pyspark4.1")
    with pytest.raises(TypeError, match="profile=.*removed"):
        kll_sketch(long(), profile=None)


def test_sketch_aggregates_are_typed_and_validate_precision():
    aggregate = kll_sketch_agg(1)
    assert isinstance(aggregate.type, KllSketchType)
    assert aggregate.type.domain == "long"
    assert isinstance(theta_sketch_agg("customer").type, ThetaSketchType)
    assert not kll_sketch_agg(1).nullable
    assert not theta_sketch_agg("customer").nullable
    assert isinstance(kll_sketch_get_n(kll_sketch_agg(1)).type, LongType)

    for invalid in (True, 7, 65536, 1.5):
        with pytest.raises(TypeError, match="k must be an integer"):
            kll_sketch_agg(1, k=cast(int, invalid))
    for invalid in (True, 3, 27, 12.0):
        with pytest.raises(TypeError, match="lg_nom_entries must be an integer"):
            theta_sketch_agg("customer", lg_nom_entries=cast(int, invalid))


def test_sketch_factories_reject_profile_and_preserve_hll_identity():
    for factory in (lambda: hll_sketch(profile=None), lambda: bitmap(profile=None), lambda: theta_sketch(profile=None)):
        with pytest.raises(TypeError, match="profile=.*removed"):
            factory()
    assert hll_sketch(lg_config_k=12).type.lg_config_k == 12


def test_sketch_capabilities_are_exact_41_for_both_variants():
    requirements = (
        CapabilityRequirement(group="schema", name="sketches_profile_4_1"),
        *(CapabilityRequirement(group="aggregate", name=name) for name in (
            "kll_sketch_agg", "theta_sketch_agg", "theta_union_agg", "theta_intersection_agg"
        )),
        *(CapabilityRequirement(group="expression", name=name) for name in (
            "kll_sketch_get_n", "kll_sketch_get_quantile", "kll_sketch_get_rank", "kll_sketch_merge",
            "kll_sketch_to_string", "theta_union", "theta_intersection", "theta_difference",
            "theta_sketch_estimate",
        )),
    )
    for variant in ("ordinary", "spark-connect"):
        for requirement in requirements:
            assert PySparkCapabilities(target_profile=">=4.1,<4.2", target_variant=variant).supports(requirement).supported
            for profile in (">=3.5,<4.1", ">=4.0,<4.1", ">=4.2,<4.3"):
                assert not PySparkCapabilities(target_profile=profile, target_variant=variant).supports(requirement).supported


def test_schema_metadata_round_trips_sketch_identity_and_user_metadata():
    class SketchSchema(Schema):
        kll = kll_sketch(double(), nullable=False, metadata={"owner": "orders"})
        theta = theta_sketch()
        hll = hll_sketch(lg_config_k=14)
        bits = bitmap()

    materialized = MaterializePySparkSchema()(SketchSchema, types=FakeTypes)
    descriptors = {field.name: field.metadata.get("structure.logical_type") for field in materialized.fields}
    assert descriptors["kll"] == {"version": 1, "kind": "kll_sketch", "domain": "double"}
    assert descriptors["theta"] == {"version": 1, "kind": "theta_sketch"}
    assert descriptors["hll"] == {"version": 1, "kind": "hll_sketch", "lg_config_k": 14}
    assert descriptors["bits"] == {"version": 1, "kind": "bitmap"}
    assert materialized.fields[0].metadata["owner"] == "orders"

    recovered = MapPySparkSchemaToStructureSource()(materialized, to="RecoveredSketches")
    recovered_fields = {field.name: field.type for field in recovered.classes[-1].fields}
    assert recovered_fields["kll"] == "kll_sketch(double())"
    assert recovered_fields["theta"] == "theta_sketch()"
    assert recovered_fields["hll"] == "hll_sketch(lg_config_k=14)"
    assert recovered_fields["bits"] == "bitmap()"

    rendered = RenderPySparkSchema()(SketchSchema)
    assert "structure.logical_type" in rendered
    assert "\"domain\": \"double\"" in rendered


def test_nested_sketch_metadata_round_trips_and_conflicts_fail():
    class NestedSketchSchema(Schema):
        values = array_field(kll_sketch(double()))
        lookup = map_field(string_field(), theta_sketch())

    materialized = MaterializePySparkSchema()(NestedSketchSchema, types=FakeTypes)
    recovered = MapPySparkSchemaToStructureSource()(materialized, to="RecoveredNestedSketches")
    fields = {field.name: field.type for field in recovered.classes[-1].fields}
    assert fields["values"] == "array(kll_sketch(double()), contains_null=True)"
    assert fields["lookup"] == "map(string(), theta_sketch(), value_contains_null=True)"

    expected = kll_sketch(double()).type
    with pytest.raises(ValueError, match="malformed structure.logical_type"):
        validate_field_metadata({"structure.logical_type": "kll"}, expected, path="values")
    with pytest.raises(ValueError, match="unsupported structure.logical_type version"):
        validate_field_metadata(
            {"structure.logical_type": {"version": 2, "kind": "kll_sketch", "domain": "double"}},
            expected,
            path="values",
        )
    with pytest.raises(ValueError, match="declares logical type"):
        validate_field_metadata(
            {"structure.logical_type": {"version": 1, "kind": "kll_sketch", "domain": "long"}},
            expected,
            path="values",
        )
