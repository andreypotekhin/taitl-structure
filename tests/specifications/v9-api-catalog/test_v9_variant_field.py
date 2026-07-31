from typing import cast

import pytest

from structure import Schema, Transform, input, output
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model import BackendCapabilityError
from structure.plugin.pyspark import (
    boolean,
    integer,
    is_valid_variant,
    is_variant_null,
    map,
    parse_json,
    schema_of_variant,
    schema_of_variant_agg,
    string,
    to_variant_object,
    try_parse_json,
    try_variant_get,
    types,
    variant,
    variant_get,
)
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.render.logic.steps.RenderPySparkStep import render_pyspark_step


class VariantInput(Schema):
    payload = variant(nullable=False)


class VariantOutput(Schema):
    payload = variant(nullable=False)


class VariantHelpersInput(Schema):
    payload = variant(nullable=True)
    payload_json = string(nullable=True)
    attributes = map(string(), string(), nullable=True)


class VariantHelpersOutput(Schema):
    parsed = variant(nullable=True)
    safe_parsed = variant(nullable=True)
    schema = string(nullable=True)
    name = string(nullable=True)
    safe_name = string(nullable=True)
    object = variant(nullable=True)
    is_valid = boolean(nullable=True)
    is_json_null = boolean(nullable=True)


class VariantSchemaSummary(Schema):
    schema = string(nullable=True)


class PreserveVariant(Transform):
    source = input(VariantInput)
    result = output(VariantOutput)

    def preserve(self, row: VariantInput) -> VariantOutput:
        return VariantOutput(payload=row.payload)


class UseVariantHelpers(Transform):
    source = input(VariantHelpersInput)
    result = output(VariantHelpersOutput)

    def convert(self, row: VariantHelpersInput) -> VariantHelpersOutput:
        return VariantHelpersOutput(
            parsed=parse_json(row.payload_json),
            safe_parsed=try_parse_json(row.payload_json),
            schema=schema_of_variant(row.payload),
            name=variant_get(row.payload, "$.name", as_type=types.string()),
            safe_name=try_variant_get(row.payload, "$.name", as_type=types.string()),
            object=to_variant_object(row.attributes),
            is_valid=is_valid_variant(row.payload),
            is_json_null=is_variant_null(row.payload),
        )


class SummarizeVariantSchema(Transform):
    source = input(VariantHelpersInput)
    result = output(VariantSchemaSummary)

    def summarize(self, row: VariantHelpersInput) -> VariantSchemaSummary:
        return VariantSchemaSummary(schema=schema_of_variant_agg(row.payload))


def test_v9_variant_field_requires_the_pyspark_4_target_profile() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        Compiler.frontend.compile()(PreserveVariant, materialize_schemas=False)

    assert raised.value.diagnostic.feature_group == "schema"
    assert raised.value.diagnostic.feature_name == "variant"


def test_v9_variant_field_uses_global_pyspark_4_profile_for_lowering() -> None:
    compilation = Compiler.frontend.compile()(
        PreserveVariant,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.2,<4.3", "variant": "ordinary"}},
    )

    lowered = cast(PySparkExecutionPlan, compilation.lowered)
    assert lowered.backend.target == ">=4.2,<4.3"
    assert lowered.steps[0].output_schema is VariantOutput


def test_v9_variant_4_2_helpers_render_as_typed_pyspark_calls() -> None:
    compilation = Compiler.frontend.compile()(
        UseVariantHelpers,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.2,<4.3", "variant": "ordinary"}},
    )

    lowered = cast(PySparkExecutionPlan, compilation.lowered)
    rendered = render_pyspark_step(lowered.steps[0], current="source", sources={"source": "source"})

    assert 'F.parse_json(F.col("variant_helpers_input.payload_json"))' in rendered
    assert 'F.try_parse_json(F.col("variant_helpers_input.payload_json"))' in rendered
    assert 'F.schema_of_variant(F.col("variant_helpers_input.payload"))' in rendered
    assert "F.variant_get(F.col(\"variant_helpers_input.payload\"), '$.name', 'string')" in rendered
    assert "F.try_variant_get(F.col(\"variant_helpers_input.payload\"), '$.name', 'string')" in rendered
    assert 'F.to_variant_object(F.col("variant_helpers_input.attributes"))' in rendered
    assert 'F.is_valid_variant(F.col("variant_helpers_input.payload"))' in rendered
    assert 'F.is_variant_null(F.col("variant_helpers_input.payload"))' in rendered


def test_v9_is_valid_variant_requires_the_pyspark_4_2_target_profile() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        Compiler.frontend.compile()(
            UseVariantHelpers,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": "ordinary"}},
        )

    assert raised.value.diagnostic.feature_group == "expression"
    assert raised.value.diagnostic.feature_name == "is_valid_variant"


def test_v9_variant_schema_aggregate_renders_as_a_pyspark_4_call() -> None:
    compilation = Compiler.frontend.compile()(
        SummarizeVariantSchema,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.2,<4.3", "variant": "ordinary"}},
    )

    lowered = cast(PySparkExecutionPlan, compilation.lowered)
    rendered = render_pyspark_step(lowered.steps[0], current="source", sources={"source": "source"})

    assert 'F.schema_of_variant_agg(F.col("variant_helpers_input.payload")).cast(T.StringType()).alias("schema")' in rendered


def test_v9_variant_schema_aggregate_requires_the_pyspark_4_target_profile() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        Compiler.frontend.compile()(SummarizeVariantSchema, materialize_schemas=False)

    assert raised.value.diagnostic.feature_group == "schema"
    assert raised.value.diagnostic.feature_name == "variant"


def test_v9_to_variant_object_rejects_non_string_map_keys() -> None:
    class InvalidInput(Schema):
        values = map(integer(), string(), nullable=True)

    with pytest.raises(TypeError, match="requires String Map keys"):
        to_variant_object(Expression(kind="test", type=InvalidInput._structure_fields["values"].type, nullable=True))
