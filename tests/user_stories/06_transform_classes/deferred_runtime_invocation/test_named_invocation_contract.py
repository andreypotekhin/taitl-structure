import pytest

from structure import StructureSession


def test_transform_construction_binds_named_inputs_without_running() -> None:
    """I can construct a transform invocation with named DataFrame inputs and run it later."""

    from testing.model.v1.orders.transforms.order import EnrichOrders

    value = object()
    invocation = EnrichOrders(
        orders=value,
        customers=value,
        products=value,
        promotions=value,
    )

    assert invocation._structure_bound_inputs == {
        "orders": value,
        "customers": value,
        "products": value,
        "promotions": value,
    }


def test_transform_invocation_rejects_unknown_input_names() -> None:
    """Unknown invocation input names fail at construction time."""

    from testing.model.v1.orders.transforms.order import EnrichOrders

    with pytest.raises(TypeError, match="unknown input"):
        EnrichOrders(order=object())


def test_run_delegates_to_structure_session_runtime() -> None:
    """I can call run(session) on a transform invocation so StructureSession chooses the runtime runner."""

    from testing.model.v1.orders.transforms.order import EnrichOrders

    captured = {}

    def executor(**kwargs):
        captured.update(kwargs)
        return "published-frame"

    frame = object()
    invocation = EnrichOrders(
        orders=frame,
        customers=frame,
        products=frame,
        promotions=frame,
    )
    session = StructureSession(schema_types=FakeTypes, online_executor=executor)

    result = invocation.run(session)

    assert result.published == "published-frame"
    assert captured["inputs"]["orders"] is frame
    assert captured["plan"].transform == "EnrichOrders"


class FakeTypes:

    @staticmethod
    def StructType(fields):
        return ("StructType", tuple(fields))

    @staticmethod
    def StructField(name, dataType, nullable):
        return ("StructField", name, dataType, nullable)

    @staticmethod
    def StringType():
        return "StringType"

    @staticmethod
    def IntegerType():
        return "IntegerType"

    @staticmethod
    def LongType():
        return "LongType"

    @staticmethod
    def FloatType():
        return "FloatType"

    @staticmethod
    def DoubleType():
        return "DoubleType"

    @staticmethod
    def BooleanType():
        return "BooleanType"

    @staticmethod
    def DateType():
        return "DateType"

    @staticmethod
    def TimestampType():
        return "TimestampType"

    @staticmethod
    def DecimalType(precision, scale):
        return ("DecimalType", precision, scale)

    @staticmethod
    def ArrayType(element, *, containsNull):
        return ("ArrayType", element, containsNull)

    @staticmethod
    def MapType(key, value, *, valueContainsNull):
        return ("MapType", key, value, valueContainsNull)
