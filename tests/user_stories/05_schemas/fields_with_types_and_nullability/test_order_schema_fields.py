import pytest
from testing.model.v1.orders.schemas.common import Address
from testing.model.v1.orders.schemas.order import OrderNormalized, OrderRaw, OrderWithCustomer

from structure import String, Structure, field


def test_fields_keep_types_nullability_and_collection_shape() -> None:
    """I can define fields with types and nullability."""

    fields = OrderRaw._structure_fields

    assert fields["id"].type.name == "string"
    assert fields["id"].nullable is False
    assert fields["id"].primary_key is True
    assert fields["total"].type.name == "string"
    assert fields["total"].nullable is True

    assert fields["tags"].type.name == "array"
    assert fields["tags"].type.element.name == "string"
    assert fields["tags"].type.contains_null is False
    assert fields["attributes"].type.name == "map"
    assert fields["attributes"].type.key.name == "string"
    assert fields["attributes"].type.value.name == "string"
    assert fields["shipping"].type.name == "struct"
    assert fields["shipping"].type.schema is Address


def test_intermediate_and_inherited_schemas_preserve_explicit_contracts() -> None:
    """Intermediate and inherited schemas preserve explicit contracts."""

    normalized = OrderNormalized._structure_fields
    assert normalized["total"].type.name == "decimal"
    assert normalized["total"].type.precision == 12
    assert normalized["total"].type.scale == 2
    assert normalized["quantity"].type.name == "long"
    assert normalized["quantity"].nullable is False

    enriched = OrderWithCustomer._structure_fields
    assert list(enriched)[: len(normalized)] == list(normalized)
    assert list(OrderWithCustomer._structure_local_fields) == [
        "customer_name",
        "customer_tier",
        "customer_region",
    ]


def test_field_aliases_define_spark_column_names_without_renaming_python_fields() -> None:
    """I can declare field aliases for non-identifier Spark column names."""

    class Raw(Structure):
        promotion_code = field(String(), nullable=True, alias="promo-code")

    field_def = Raw._structure_fields["promotion_code"]

    assert field_def.name == "promotion_code"
    assert field_def.alias == "promo-code"
    assert field_def.column == "promo-code"


def test_aliases_are_schema_local_but_inherited_with_field_contracts() -> None:
    """Aliases belong to the declaring schema unless inherited."""

    class Raw(Structure):
        promotion_code = field(String(), nullable=True, alias="promo-code")

    class Normalized(Structure):
        promotion_code = field(String(), nullable=True)

    class StillRaw(Raw):
        pass

    assert Raw._structure_fields["promotion_code"].column == "promo-code"
    assert Normalized._structure_fields["promotion_code"].column == "promotion_code"
    assert StillRaw._structure_fields["promotion_code"].column == "promo-code"


def test_invalid_and_duplicate_aliases_fail_early() -> None:
    """Aliases must be useful Spark column names."""

    with pytest.raises(ValueError, match="field alias must be a non-empty string"):
        field(String(), alias="")

    with pytest.raises(ValueError, match="field alias must be a non-empty string"):
        field(String(), alias=123)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="duplicate Spark column name 'promo-code'"):

        class Duplicate(Structure):
            promotion_code = field(String(), alias="promo-code")
            alternate_code = field(String(), alias="promo-code")
