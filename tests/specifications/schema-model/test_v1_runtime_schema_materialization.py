import sys
from dataclasses import dataclass

from structure.app.target.pyspark.api import materialize_pyspark_schema


@dataclass(frozen=True)
class FakeType:
    name: str
    args: tuple = ()
    options: tuple = ()


class FakeTypes:

    @staticmethod
    def StructType(fields):
        return FakeType("StructType", tuple(fields))

    @staticmethod
    def StructField(name, dataType, nullable):
        return FakeType("StructField", (name, dataType, nullable))

    @staticmethod
    def StringType():
        return FakeType("StringType")

    @staticmethod
    def IntegerType():
        return FakeType("IntegerType")

    @staticmethod
    def LongType():
        return FakeType("LongType")

    @staticmethod
    def FloatType():
        return FakeType("FloatType")

    @staticmethod
    def DoubleType():
        return FakeType("DoubleType")

    @staticmethod
    def BooleanType():
        return FakeType("BooleanType")

    @staticmethod
    def DateType():
        return FakeType("DateType")

    @staticmethod
    def TimestampType():
        return FakeType("TimestampType")

    @staticmethod
    def DecimalType(precision, scale):
        return FakeType("DecimalType", (precision, scale))

    @staticmethod
    def ArrayType(element, *, containsNull):
        return FakeType("ArrayType", (element,), (("containsNull", containsNull),))

    @staticmethod
    def MapType(key, value, *, valueContainsNull):
        return FakeType("MapType", (key, value), (("valueContainsNull", valueContainsNull),))


def test_v1_runtime_schema_materialization_is_import_safe_with_injected_types() -> None:
    from testing.model.v1.orders.schemas.common import TenantKey

    before = {name for name in sys.modules if name.startswith("pyspark")}

    schema = materialize_pyspark_schema(TenantKey, types=FakeTypes)

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert schema == FakeType(
        "StructType",
        (
            FakeType(
                "StructField",
                ("tenant_id", FakeType("StringType"), False),
            ),
        ),
    )


def test_v1_runtime_schema_materializes_collections_decimal_and_nested_structs() -> None:
    from testing.model.v1.orders.schemas.customer import Customer
    from testing.model.v1.orders.schemas.order import OrderRaw

    order = materialize_pyspark_schema(OrderRaw, types=FakeTypes)
    fields = {field.args[0]: field for field in order.args}

    assert fields["tags"].args[1] == FakeType(
        "ArrayType",
        (FakeType("StringType"),),
        (("containsNull", False),),
    )
    assert fields["attributes"].args[1] == FakeType(
        "MapType",
        (FakeType("StringType"), FakeType("StringType")),
        (("valueContainsNull", True),),
    )
    assert fields["total"].args[1] == FakeType("StringType")

    customer = materialize_pyspark_schema(Customer, types=FakeTypes)
    customer_fields = {field.args[0]: field for field in customer.args}

    assert customer_fields["tenant"].args[1] == FakeType(
        "StructType",
        (
            FakeType(
                "StructField",
                ("tenant_id", FakeType("StringType"), False),
            ),
        ),
    )


def test_v1_runtime_schema_materializes_effective_inherited_fields() -> None:
    from testing.model.v1.orders.schemas.order import OrderPublication, OrderPublished, PublicationFlags

    schema = materialize_pyspark_schema(OrderPublished, types=FakeTypes)
    names = [field.args[0] for field in schema.args]
    flag_names = list(PublicationFlags._structure_fields)
    flag_start = len(names) - len(flag_names)

    assert names == list(OrderPublished._structure_fields)
    assert names[: len(OrderPublication._structure_fields)] == list(OrderPublication._structure_fields)
    assert names[flag_start:] == flag_names
