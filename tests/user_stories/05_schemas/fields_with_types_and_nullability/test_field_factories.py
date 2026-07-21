from datetime import datetime as DateTime
from decimal import Decimal as DecimalValue
from typing import Any, cast

import pytest

import structure
from structure import Schema
from structure.core.dsl.model.types.Array import Array
from structure.core.dsl.model.types.Decimal import Decimal
from structure.core.dsl.model.types.Map import Map
from structure.plugin.pyspark import *
from structure.plugin.pyspark.dsl.ValidatePySparkSchemas import ValidatePySparkSchemas


def test_schema_module_wildcard_factories_keep_type_and_nullability_contracts() -> None:
    class Order(Schema):
        id = string(nullable=False)
        promo_code = string(alias="promo-code")
        total = decimal(12, 2, nullable=False)
        tags = array(string(), contains_null=False)
        attributes = map(string(), string(), value_contains_null=False)

    fields = cast(dict[str, Any], Order._structure_fields)

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
        tags = array(string(), contains_null=False)

    expression = array("priority", "standard")

    assert isinstance(Source._structure_fields["tags"].type, Array)
    assert Source._structure_fields["tags"].type.contains_null is False
    assert expression.type is not None
    assert expression.type.name == "array"


def test_python_hints_infer_default_pyspark_fields() -> None:
    class Address(Schema):
        street: str

    class Order(Schema):
        name: str
        count: int
        ratio: float
        active: bool
        ordered_on: date
        observed_at: DateTime
        tags: list[str]
        attributes: dict[str, int]
        address: Address

    ValidatePySparkSchemas().validate(Order, Order._structure_fields)

    fields = cast(dict[str, Any], Order._structure_fields)
    assert [fields[name].type.name for name in fields] == [
        "string", "integer", "double", "boolean", "date", "timestamp", "array", "map", "struct"
    ]
    assert fields["tags"].type.element.name == "string"
    assert fields["attributes"].type.key.name == "string"
    assert fields["attributes"].type.value.name == "integer"
    assert fields["address"].type.schema is Address
    assert all(field.nullable for field in fields.values())


def test_hints_accept_compatible_factory_detail() -> None:
    class Price(Schema):
        amount: DecimalValue = decimal(12, 2, nullable=False)
        quantity: int = long(nullable=False)
        ratio: float = float(nullable=False)
        tags: list[str] = array(string(), contains_null=False, nullable=False)

    fields = cast(dict[str, Any], Price._structure_fields)
    assert fields["amount"].type.name == "decimal"
    assert fields["quantity"].type.name == "long"
    assert fields["ratio"].type.name == "float"
    assert fields["tags"].type.contains_null is False


def test_hints_reject_incompatible_factory_detail() -> None:
    with pytest.raises(TypeError, match="Price.amount hint float is incompatible with decimal\\(12, 2\\)"):
        class Price(Schema):
            amount: float = decimal(12, 2)


@pytest.mark.parametrize("hint", [DecimalValue, str | None, list, dict])
def test_bare_hints_reject_unsupported_or_under_specified_shapes(hint: object) -> None:
    class Invalid(Schema):
        value: hint  # type: ignore[valid-type]

    with pytest.raises(TypeError):
        ValidatePySparkSchemas().validate(Invalid, Invalid._structure_fields)


def test_annotated_ordinary_assignments_remain_schema_constants() -> None:
    class Versioned(Schema):
        version: str = "v1"
        identifier: str

    ValidatePySparkSchemas().validate(Versioned, Versioned._structure_fields)

    assert Versioned.version == "v1"
    assert tuple(Versioned._structure_fields) == ("identifier",)
