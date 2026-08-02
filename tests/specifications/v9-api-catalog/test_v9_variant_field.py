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
    long,
    map,
    parse_json,
    schema_of_variant,
    schema_of_variant_agg,
    string,
    to_variant_object,
    try_parse_json,
    try_variant_array_append,
    try_variant_get,
    try_variant_insert,
    try_variant_set,
    types,
    variant,
    variant_array_append,
    variant_delete,
    variant_explode,
    variant_explode_outer,
    variant_get,
    variant_insert,
    variant_literal,
    variant_set,
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
    literal_value = variant(nullable=True)
    same_variant = boolean(nullable=True)


class VariantSchemaSummary(Schema):
    schema = string(nullable=True)


class VariantMutationsOutput(Schema):
    appended = variant(nullable=True)
    safe_appended = variant(nullable=True)
    inserted = variant(nullable=True)
    safe_inserted = variant(nullable=True)
    set_value = variant(nullable=True)
    safe_set_value = variant(nullable=True)


class VariantDeleteOutput(Schema):
    deleted = variant(nullable=True)


class VariantEntry(Schema):
    pos = long(nullable=False)
    key = string(nullable=True)
    value = variant(nullable=False)


class VariantOuterEntry(Schema):
    pos = long(nullable=True)
    key = string(nullable=True)
    value = variant(nullable=True)


class VariantExplodeOutput(Schema):
    pos = long(nullable=False)
    key = string(nullable=True)
    value = variant(nullable=False)


class VariantExplodeOuterOutput(Schema):
    pos = long(nullable=True)
    key = string(nullable=True)
    value = variant(nullable=True)


class VariantBooleanOutput(Schema):
    value = boolean(nullable=True)


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
            literal_value=variant_literal('{"source":"migration"}'),
            same_variant=row.payload == row.payload,
        )


class SummarizeVariantSchema(Transform):
    source = input(VariantHelpersInput)
    result = output(VariantSchemaSummary)

    def summarize(self, row: VariantHelpersInput) -> VariantSchemaSummary:
        return VariantSchemaSummary(schema=schema_of_variant_agg(row.payload))


class UseVariantMutations(Transform):
    source = input(VariantInput)
    result = output(VariantMutationsOutput)

    def mutate(self, row: VariantInput) -> VariantMutationsOutput:
        return VariantMutationsOutput(
            appended=variant_array_append(row.payload, "$.items", 1),
            safe_appended=try_variant_array_append(row.payload, "$.items", 1),
            inserted=variant_insert(row.payload, "$.name", "spark"),
            safe_inserted=try_variant_insert(row.payload, "$.name", "spark"),
            set_value=variant_set(row.payload, "$.name", "spark", create_if_missing=False),
            safe_set_value=try_variant_set(row.payload, "$.name", "spark"),
        )


class UseVariantDelete(Transform):
    source = input(VariantInput)
    result = output(VariantDeleteOutput)

    def delete(self, row: VariantInput) -> VariantDeleteOutput:
        return VariantDeleteOutput(deleted=variant_delete(row.payload, "$.name"))


class CompareVariantOrdering(Transform):
    source = input(VariantInput)
    result = output(VariantBooleanOutput)

    def compare(self, row: VariantInput) -> VariantBooleanOutput:
        return VariantBooleanOutput(value=row.payload > row.payload)


class UseVariantExplode(Transform):
    source = input(VariantInput)
    result = output(VariantExplodeOutput)

    def expand(self, row: VariantInput) -> VariantExplodeOutput:
        entry = variant_explode(row.payload, as_=VariantEntry)
        return VariantExplodeOutput(pos=entry.pos, key=entry.key, value=entry.value)


class UseVariantExplodeOuter(Transform):
    source = input(VariantInput)
    result = output(VariantExplodeOuterOutput)

    def expand(self, row: VariantInput) -> VariantExplodeOuterOutput:
        entry = variant_explode_outer(row.payload, as_=VariantOuterEntry)
        return VariantExplodeOuterOutput(pos=entry.pos, key=entry.key, value=entry.value)


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
    assert 'F.parse_json(F.lit(\'{"source":"migration"}\'))' in rendered
    assert 'F.col("variant_helpers_input.payload") == F.col("variant_helpers_input.payload")' in rendered


def test_v9_variant_literal_rejects_invalid_json() -> None:
    with pytest.raises(ValueError, match="requires valid JSON text"):
        variant_literal("{invalid")


def test_v9_variant_paths_and_mutation_options_are_literal_contracts() -> None:
    expression = Expression(kind="literal", type=types.variant(), nullable=False)

    with pytest.raises(TypeError, match="path must be a non-empty string literal"):
        variant_get(expression, 1, as_type=types.string())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=r"must start with '\$'"):
        variant_array_append(expression, "items", 1)
    with pytest.raises(ValueError, match="must identify a field or array element"):
        variant_delete(expression, "$")
    with pytest.raises(TypeError, match="create_if_missing must be a Boolean literal"):
        variant_set(expression, "$.name", "spark", create_if_missing=1)  # type: ignore[arg-type]


def test_v9_variant_ordering_is_rejected_but_equality_is_typed() -> None:
    with pytest.raises(TypeError, match="Ordering comparisons require orderable"):
        Compiler.frontend.compile()(
            CompareVariantOrdering,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.2,<4.3", "variant": "ordinary"}},
        )


def test_v9_variant_mutations_are_deferred_until_pyspark_4_3_is_released() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        Compiler.frontend.compile()(
            UseVariantMutations,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.2,<4.3", "variant": "ordinary"}},
        )
    assert raised.value.diagnostic.feature_name == "variant_array_append"


def test_v9_variant_delete_is_deferred_until_a_later_spark_profile_is_released() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        Compiler.frontend.compile()(
            UseVariantDelete,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.2,<4.3", "variant": "ordinary"}},
        )
    assert raised.value.diagnostic.feature_name == "variant_delete"


def test_v9_variant_explode_uses_the_pyspark_4_tvf_and_lateral_join() -> None:
    with pytest.raises(BackendCapabilityError) as raised:
        Compiler.frontend.compile()(
            UseVariantExplode,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=3.5,<4.0", "variant": "ordinary"}},
        )
    assert raised.value.diagnostic.feature_name == "variant"

    compilation = Compiler.frontend.compile()(
        UseVariantExplode,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": "ordinary"}},
    )
    lowered = cast(PySparkExecutionPlan, compilation.lowered)
    rendered = render_pyspark_step(lowered.steps[0], current="source", sources={"source": "source"})

    assert "source = source.lateralJoin(" in rendered
    assert 'self.spark.tvf.variant_explode(F.col("variant_input.payload").outer())' in rendered
    assert 'how="cross"' in rendered
    assert 'F.col("pos").alias("__structure_variantEntry_1_pos")' in rendered
    assert 'F.col("value").alias("__structure_variantEntry_1_item")' in rendered


def test_v9_variant_explode_outer_uses_the_outer_tvf() -> None:
    compilation = Compiler.frontend.compile()(
        UseVariantExplodeOuter,
        materialize_schemas=False,
        plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": "ordinary"}},
    )
    lowered = cast(PySparkExecutionPlan, compilation.lowered)
    rendered = render_pyspark_step(lowered.steps[0], current="source", sources={"source": "source"})
    assert 'self.spark.tvf.variant_explode_outer(F.col("variant_input.payload").outer())' in rendered
    assert 'how="left"' in rendered


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
