import pytest

from structure import *
from structure.core.dsl.model.expr.Expression import Expression
from structure.core.dsl.model.types.ArrayType import ArrayType
from structure.core.dsl.model.types.MapType import MapType
from structure.core.dsl.model.types.StructType import StructType
from structure.core.dsl.model.types.StructureType import StructureType


class MapEntry(Schema):
    key = field.string(nullable=False)
    value = field.long(nullable=True)


class NullableMapEntry(Schema):
    key = field.string(nullable=True)
    value = field.long(nullable=True)


class ExtendedMapEntry(Schema):
    key = field.string(nullable=False)
    value = field.long(nullable=True)
    source = field.string(nullable=False)


def _map(key: StructureType, value: StructureType, *, value_contains_null: bool = False) -> Expression:
    return Expression(
        kind="test_map",
        type=types.map(key, value, value_contains_null=value_contains_null),
        nullable=False,
    )


def test_array_aggregate_requires_a_type_stable_accumulator() -> None:
    with pytest.raises(
        TypeError, match=r"arr_aggregate\(\.\.\.\) merge callback must return the initial accumulator type"
    ):
        arr_aggregate(array("a"), 0, lambda accumulator, item: item)
    with pytest.raises(
        TypeError, match=r"arr_aggregate\(\.\.\.\) merge callback must return the initial accumulator type"
    ):
        arr_aggregate(array(2**31), 0, lambda accumulator, item: accumulator + item)


def test_array_aggregate_propagates_input_and_empty_initial_nullability() -> None:
    nullable_array = Expression(
        kind="test_nullable_array",
        type=types.array(types.integer(), contains_null=False),
        nullable=True,
    )
    required_array = Expression(
        kind="test_required_array",
        type=types.array(types.integer(), contains_null=False),
        nullable=False,
    )
    nullable_initial = Expression(kind="test_nullable_initial", type=types.integer(), nullable=True)

    assert arr_aggregate(nullable_array, 0, lambda accumulator, item: accumulator + item).nullable is True
    assert (
        arr_aggregate(required_array, nullable_initial, lambda accumulator, item: coalesce(accumulator, 0)).nullable
        is True
    )
    assert (
        arr_aggregate(
            nullable_array,
            0,
            lambda accumulator, item: accumulator + item,
            finish=lambda accumulator: coalesce(accumulator, 0),
        ).nullable
        is True
    )


def test_array_aggregate_finish_receives_a_nullable_merge_accumulator() -> None:
    nullable_items = Expression(
        kind="test_nullable_items",
        type=types.array(types.integer(), contains_null=True),
        nullable=False,
    )

    result = arr_aggregate(
        nullable_items,
        0,
        lambda accumulator, item: item,
        finish=lambda accumulator: accumulator,
    )

    assert result.nullable is True


def test_array_position_requires_an_item_compatible_with_the_array_element() -> None:
    with pytest.raises(TypeError, match=r"arr_position\(\.\.\.\) requires compatible types"):
        arr_position(array("priority"), 1)


def test_array_position_requires_a_literal_item_for_pyspark_35_compatibility() -> None:
    item = Expression(kind="test_item", type=types.string(), nullable=False)

    with pytest.raises(TypeError, match=r"arr_position\(\.\.\.\) item must be a Python literal"):
        arr_position(array("priority"), item)


def test_array_position_is_required_when_its_array_is_required() -> None:
    required = arr_position(array("priority"), "missing")
    nullable_array = Expression(
        kind="test_nullable_array",
        type=types.array(types.string(), contains_null=True),
        nullable=True,
    )

    assert required.nullable is False
    assert arr_position(nullable_array, "missing").nullable is True


def test_map_contains_key_requires_a_literal_key_for_pyspark_35_compatibility() -> None:
    key = Expression(kind="test_key", type=types.string(), nullable=False)

    with pytest.raises(TypeError, match=r"map_contains_key\(\.\.\.\) key must be a Python literal"):
        map_contains_key(_map(types.string(), types.string()), key)


def test_array_sort_by_rejects_collection_keys() -> None:
    with pytest.raises(TypeError, match=r"arr_sort_by\(\.\.\.\) callback must return an orderable scalar expression"):
        arr_sort_by(array("priority"), lambda item: array(item))


def test_array_sort_by_rejects_boolean_keys() -> None:
    with pytest.raises(TypeError, match=r"arr_sort_by\(\.\.\.\) callback must return an orderable scalar expression"):
        arr_sort_by(array("priority"), lambda item: item.is_not_null())


def test_array_flatten_is_nullable_when_a_nested_array_can_be_null() -> None:
    nested = Expression(
        kind="test_nested_array",
        type=types.array(types.array(types.string(), contains_null=False), contains_null=True),
        nullable=False,
    )

    flattened = arr_flatten(nested)

    assert flattened.nullable is True
    assert isinstance(flattened.type, ArrayType)
    assert flattened.type.element == types.string()
    assert flattened.type.contains_null is False


@pytest.mark.parametrize("helper", [arr_exists, arr_forall])
def test_array_predicates_propagate_element_and_callback_nullability(helper) -> None:
    nullable_items = Expression(
        kind="test_nullable_items",
        type=types.array(types.string(), contains_null=True),
        nullable=False,
    )
    required_items = Expression(
        kind="test_required_items",
        type=types.array(types.string(), contains_null=False),
        nullable=False,
    )
    nullable_predicate = Expression(kind="test_nullable_predicate", type=types.boolean(), nullable=True)

    assert helper(nullable_items, lambda item: item == "priority").nullable is True
    assert helper(required_items, lambda _: nullable_predicate).nullable is True
    assert helper(required_items, lambda item: item == "priority").nullable is False


def test_collection_aggregates_exclude_null_input_elements() -> None:
    nullable_value = Expression(kind="test_nullable_value", type=types.string(), nullable=True)
    frame = window(
        partition_by="tenant",
        order_by="ordered",
        frame=rows_between(current_row(), current_row()),
    )

    expressions = (
        collect_list(nullable_value),
        collect_set(nullable_value),
        window_collect_list(nullable_value, over=frame),
        window_collect_set(nullable_value, over=frame),
    )

    assert all(
        isinstance(expression.type, ArrayType) and not expression.type.contains_null and not expression.nullable
        for expression in expressions
    )


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda: approx_count_distinct("order", relative_sd=0), r"relative_sd must be a finite number"),
        (lambda: approx_count_distinct("order", relative_sd=0.4), r"relative_sd must be a finite number"),
        (lambda: approx_percentile(1, 1.1), r"percentage must be a finite number"),
        (lambda: approx_percentile(1, 0.5, accuracy=0), r"accuracy must be a positive integer"),
    ],
)
def test_approximate_aggregates_reject_values_outside_spark_limits(call, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        call()


def test_window_options_reject_boolean_values() -> None:
    spec = window(partition_by="tenant", order_by="ordered")

    with pytest.raises(TypeError, match=r"preceding\(\.\.\.\) value must be an integer greater than or equal to 0"):
        preceding(True)
    with pytest.raises(TypeError, match=r"ntile\(\.\.\.\) value must be a positive integer"):
        ntile(True, over=spec)
    with pytest.raises(TypeError, match=r"lag\(\.\.\.\) offset must be a positive integer"):
        lag(1, partition_by="tenant", order_by="ordered", offset=True)


def test_map_transform_keys_rejects_nullable_callback_results() -> None:
    with pytest.raises(TypeError, match=r"map_transform_keys\(\.\.\.\) callback must return a non-null key expression"):
        map_transform_keys(
            _map(types.string(), types.string(), value_contains_null=True),
            lambda key, value: value,
        )


def test_map_zip_with_requires_matching_key_types() -> None:
    with pytest.raises(TypeError, match=r"map_zip_with\(\.\.\.\) requires matching map key types"):
        map_zip_with(
            _map(types.integer(), types.string()),
            _map(types.long(), types.string()),
            lambda key, left, right: left,
        )


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (_map(types.integer(), types.string()), _map(types.long(), types.string())),
        (_map(types.string(), types.integer()), _map(types.string(), types.long())),
    ],
)
def test_map_concat_requires_matching_key_and_value_types(left: Expression, right: Expression) -> None:
    with pytest.raises(TypeError, match=r"map_concat\(\.\.\.\) requires maps with matching key and value types"):
        map_concat(left, right)


def test_map_keys_cannot_contain_maps() -> None:
    with pytest.raises(ValueError, match="MapType key cannot contain another MapType"):
        types.map(types.map(types.string(), types.string()), types.string())
    with pytest.raises(ValueError, match="MapType key cannot contain another MapType"):
        types.map(types.array(types.map(types.string(), types.string())), types.string())


def test_map_entries_preserves_key_and_value_types_for_round_tripping() -> None:
    original = _map(types.string(), types.long(), value_contains_null=True)

    entries = map_entries(original)
    restored = map_from_entries(entries)

    assert isinstance(entries.type, ArrayType)
    assert isinstance(entries.type.element, StructType)
    fields = entries.type.element.schema._structure_fields
    assert fields["key"].type == types.string()
    assert fields["key"].nullable is False
    assert fields["value"].type == types.long()
    assert fields["value"].nullable is True
    assert isinstance(restored.type, MapType)
    assert isinstance(original.type, MapType)
    assert restored.type.key == original.type.key
    assert restored.type.value == original.type.value
    assert restored.type.value_contains_null is True


def test_map_from_entries_requires_non_null_key_value_struct_entries() -> None:
    invalid_entries = Expression(
        kind="test_entries",
        type=types.array(types.struct(MapEntry), contains_null=False),
        nullable=False,
    )
    valid_entries = map_from_entries(invalid_entries)

    assert isinstance(valid_entries.type, MapType)
    assert valid_entries.type.key == types.string()
    assert valid_entries.type.value == types.long()
    assert valid_entries.type.value_contains_null is True

    with pytest.raises(TypeError, match=r"map_from_entries\(\.\.\.\) requires an Array of key/value Struct entries"):
        map_from_entries(array("not-an-entry"))


@pytest.mark.parametrize(
    ("entry_type", "message"),
    [
        (types.struct(NullableMapEntry), r"map_from_entries\(\.\.\.\) requires non-null key fields"),
        (types.struct(ExtendedMapEntry), r"map_from_entries\(\.\.\.\) requires Struct entries with exactly key"),
    ],
)
def test_map_from_entries_rejects_ambiguous_struct_entry_shapes(entry_type, message: str) -> None:
    entries = Expression(kind="test_entries", type=types.array(entry_type, contains_null=False), nullable=False)

    with pytest.raises(TypeError, match=message):
        map_from_entries(entries)
