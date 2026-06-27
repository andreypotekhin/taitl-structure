import sys
from dataclasses import dataclass
from types import ModuleType

import pytest

from structure import StructureRuntimeError, StructureSession


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


def test_v1_online_session_defers_to_runner_and_exposes_schemas_without_pyspark() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    before = {name for name in sys.modules if name.startswith("pyspark")}
    captured = {}

    def executor(**kwargs):
        captured.update(kwargs)
        return "online-result"

    invocation = EnrichOrders(
        orders="orders-df",
        customers="customers-df",
        products="products-df",
        promotions="promotions-df",
    )
    session = StructureSession(spark="spark", ctx="ctx", schema_types=FakeTypes, online_executor=executor)

    result = invocation.run(session)

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert result.published == "online-result"
    assert result["published"] == "online-result"
    assert captured["spark"] == "spark"
    assert captured["ctx"] == "ctx"
    assert captured["inputs"]["orders"] == "orders-df"
    assert captured["plan"].transform == "EnrichOrders"
    assert list(result.schema) == ["published"]
    assert result.schema.published.name == "StructType"
    assert result.schema["published"].name == "StructType"


def test_v1_online_session_reports_missing_declared_inputs() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    session = StructureSession(schema_types=FakeTypes, online_executor=lambda **kwargs: None)

    with pytest.raises(StructureRuntimeError) as raised:
        session.run(EnrichOrders(orders="orders-df"))

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "ONLINE-E1201"
    assert diagnostic.execution_mode == "online"
    assert diagnostic.context["inputs"] == "customers, products, promotions"
    assert "Pass every declared input DataFrame" in diagnostic.use
    assert "docs/Diagnostics.md#online-e1201" in str(raised.value)


def test_v1_generated_session_delegates_to_generated_class() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    module_name = "testing.model.v1.structure_generated.orders.pyspark.transforms.order"
    installed = _install_generated_module(module_name)
    try:
        invocation = EnrichOrders(
            orders="orders-df",
            customers="customers-df",
            products="products-df",
            promotions="promotions-df",
        )
        session = StructureSession(
            spark="spark",
            ctx="ctx",
            execution_mode="generated",
            generated_package="testing.model.v1.structure_generated.orders",
            schema_types=FakeTypes,
        )

        result = session.run(invocation)

        assert result.published == {
            "spark": "spark",
            "ctx": "ctx",
            "orders": "orders-df",
            "customers": "customers-df",
            "products": "products-df",
            "promotions": "promotions-df",
        }
        assert result.schema.published.name == "StructType"
    finally:
        for name in installed:
            sys.modules.pop(name, None)


def test_v1_generated_session_reports_missing_generated_code() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    invocation = EnrichOrders(
        orders="orders-df",
        customers="customers-df",
        products="products-df",
        promotions="promotions-df",
    )
    session = StructureSession(
        execution_mode="generated",
        generated_package="missing_structure_generated",
        schema_types=FakeTypes,
    )

    with pytest.raises(StructureRuntimeError) as raised:
        session.run(invocation)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "GEN-E0902"
    assert diagnostic.execution_mode == "generated"
    assert "structure compile" in diagnostic.use
    assert "missing_structure_generated.pyspark.transforms.order" in diagnostic.problem


def _install_generated_module(name: str) -> list[str]:
    installed: list[str] = []
    parts = name.split(".")
    for index in range(1, len(parts)):
        package_name = ".".join(parts[:index])
        if package_name not in sys.modules:
            package = ModuleType(package_name)
            package.__path__ = []  # type: ignore[attr-defined]
            sys.modules[package_name] = package
            installed.append(package_name)
        if index > 1:
            parent = sys.modules[".".join(parts[: index - 1])]
            setattr(parent, parts[index - 1], sys.modules[package_name])

    module = ModuleType(name)

    class EnrichOrdersGenerated:

        def __init__(self, *, spark, ctx=None) -> None:
            self.spark = spark
            self.ctx = ctx

        def run(self, *, orders, customers, products, promotions):
            return {
                "spark": self.spark,
                "ctx": self.ctx,
                "orders": orders,
                "customers": customers,
                "products": products,
                "promotions": promotions,
            }

    setattr(module, "EnrichOrdersGenerated", EnrichOrdersGenerated)
    sys.modules[name] = module
    setattr(sys.modules[".".join(parts[:-1])], parts[-1], module)
    installed.append(name)
    return list(reversed(installed))
