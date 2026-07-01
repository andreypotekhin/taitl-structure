import importlib
import sys
import types
from typing import Any, cast

import pytest

from structure import Long, String, Structure, Transform, count, field, group_by, input, output, transform
from structure.app.dsl.api import compile_transform
from structure.app.target.capabilities.api import BackendCapabilityError
from structure.app.target.pyspark.api import PySpark


def test_v2_source_fixtures_import_without_live_spark(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pyspark(monkeypatch)
    before = {name for name in sys.modules if name.startswith("pyspark")}

    for module in (
        "testing.model.v2.orders.schemas.analytics",
        "testing.model.v2.orders.schemas.common",
        "testing.model.v2.orders.schemas.customer",
        "testing.model.v2.orders.schemas.order",
        "testing.model.v2.orders.schemas.product",
        "testing.model.v2.orders.schemas.promotion",
        "testing.model.v2.orders.schemas.shipment",
        "testing.model.v2.orders.transforms.analytics",
        "testing.model.v2.orders.transforms.order",
    ):
        importlib.import_module(module)

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before


def test_reserved_group_by_fails_through_backend_capability() -> None:
    class Raw(Structure):
        customer_id = field(String(), nullable=False)
        quantity = field(Long(), nullable=False)

    class Total(Structure):
        customer_id = field(String(), nullable=False)
        quantity = field(Long(), nullable=False)

    @transform
    class Totals(Transform):
        rows = input(Raw)
        totals = output(Total)

        def total(self, row: Raw) -> Total:
            group_by(customer_id=row.customer_id)
            return Total(customer_id=row.customer_id, quantity=count())

    with pytest.raises(BackendCapabilityError) as raised:
        PySpark.plan.lower()(compile_transform(Totals))

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "BACKEND-E2402"
    assert diagnostic.feature_group == "aggregate"
    assert diagnostic.feature_name == "group_by"


def _stub_pyspark(monkeypatch: pytest.MonkeyPatch) -> None:
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")

    class StorageLevel:
        MEMORY_AND_DISK = object()

    def expression_function(*args: object, **kwargs: object) -> object:
        return object()

    setattr(functions, "col", expression_function)
    setattr(functions, "lit", expression_function)
    setattr(pyspark, "StorageLevel", StorageLevel)
    setattr(sql, "functions", functions)
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
