from testing.model.v1.orders.schemas.common import Address
from testing.model.v1.orders.schemas.order import OrderNormalized, OrderRaw, OrderWithCustomer


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
