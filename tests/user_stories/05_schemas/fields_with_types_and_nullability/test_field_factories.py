import pytest

import structure
from structure import Schema
from structure import array as expression_array
from structure.core.dsl.model.types.Array import Array
from structure.core.dsl.model.types.Decimal import Decimal
from structure.core.dsl.model.types.Map import Map
from structure.platform.pyspark import types
from structure.platform.pyspark.dsl.field import *
from structure.platform.pyspark.dsl.field import array as field_array
from structure.platform.pyspark.dsl.field import string as field_string


def test_schema_module_wildcard_factories_keep_type_and_nullability_contracts() -> None:
    class Order(Schema):
        id = string(nullable=False)
        promo_code = string(alias="promo-code")
        total = decimal(12, 2, nullable=False)
        tags = array(string(), contains_null=False)
        attributes = map(string(), string(), value_contains_null=False)

    fields = Order._structure_fields

    assert fields["id"].type.name == "string"
    assert fields["id"].nullable is False
    assert fields["promo_code"].column == "promo-code"
    assert fields["promo_code"].nullable is True
    assert isinstance(fields["total"].type, Decimal)
    assert (fields["total"].type.precision, fields["total"].type.scale) == (12, 2)
    assert isinstance(fields["tags"].type, Array)
    assert fields["tags"].type.contains_null is False
    assert isinstance(fields["attributes"].type, Map)
    assert fields["attributes"].type.value_contains_null is False


def test_nested_factory_rejects_field_nullability() -> None:
    with pytest.raises(TypeError, match="contains_null=False"):
        array(string(nullable=False))
    with pytest.raises(TypeError, match="contains_null=False"):
        array(string(nullable=True))


def test_one_declaration_can_bind_to_multiple_schema_fields() -> None:
    identifier = string(nullable=False, alias="identifier")

    class Customer(Schema):
        id = identifier

    class Product(Schema):
        id = identifier

    assert Customer._structure_fields["id"].column == "identifier"
    assert Product._structure_fields["id"].column == "identifier"
    assert Customer._structure_fields["id"].name == "id"
    assert Product._structure_fields["id"].name == "id"


def test_legacy_constructors_are_not_root_exports() -> None:
    assert not hasattr(structure, "String")
    assert not hasattr(structure, "field")
    assert not hasattr(structure, "types")


def test_standalone_type_factories_build_composable_type_values() -> None:
    nested = types.array(types.decimal(12, 2), contains_null=False)

    assert isinstance(nested, Array)
    assert nested.element == types.decimal(12, 2)
    assert nested.contains_null is False


@pytest.mark.parametrize("value", [1, "false", None])
def test_collection_type_nullability_requires_a_boolean(value: object) -> None:
    with pytest.raises(TypeError, match="ArrayType contains_null must be a Boolean"):
        types.array(types.string(), contains_null=value)
    with pytest.raises(TypeError, match="MapType value_contains_null must be a Boolean"):
        types.map(types.string(), types.string(), value_contains_null=value)


def test_schema_array_factory_coexists_with_array_expression_helper() -> None:
    class Source(Schema):
        tags = field_array(field_string(), contains_null=False)

    expression = expression_array("priority", "standard")

    assert isinstance(Source._structure_fields["tags"].type, Array)
    assert Source._structure_fields["tags"].type.contains_null is False
    assert expression.type is not None
    assert expression.type.name == "array"
