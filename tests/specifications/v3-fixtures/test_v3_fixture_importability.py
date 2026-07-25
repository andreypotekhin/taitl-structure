import importlib
import sys
import types as python_types
from ast import parse
from pathlib import Path
from typing import Any, cast

import pytest

from structure import *
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark import *
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


def test_v3_source_fixtures_import_without_live_spark(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pyspark(monkeypatch)
    before = {name for name in sys.modules if name.startswith("pyspark")}

    for module in (
        "testing.model.v3.orders.schemas.adv_analytics",
        "testing.model.v3.orders.schemas.analytics",
        "testing.model.v3.orders.schemas.common",
        "testing.model.v3.orders.schemas.customer",
        "testing.model.v3.orders.schemas.order",
        "testing.model.v3.orders.schemas.product",
        "testing.model.v3.orders.schemas.promotion",
        "testing.model.v3.orders.schemas.shipment",
        "testing.model.v3.orders.schemas.v3",
        "testing.model.v3.orders.transforms.adv_analytics",
        "testing.model.v3.orders.transforms.analytics",
        "testing.model.v3.orders.transforms.order",
        "testing.model.v3.orders.transforms.rowset_join",
        "testing.model.v3.orders.transforms.v3",
    ):
        importlib.import_module(module)

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before


def test_v3_orders_fixture_highlights_the_completed_release_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pyspark(monkeypatch)
    scalar = importlib.import_module("testing.model.v3.orders.transforms.v3")
    analytics = importlib.import_module("testing.model.v3.orders.transforms.adv_analytics")

    scalar_plan = cast(
        TransformPlan,
        Compiler.frontend.compile()(
            cast(Any, scalar).V3OrderFeatures,
            materialize_schemas=False,
            plugin={"pyspark": {"profile": ">=4.0,<4.1"}},
        ).analysis,
    )
    analytics_plan = cast(
        TransformPlan,
        Compiler.frontend.compile()(cast(Any, analytics).AdvancedOrderAnalytics, materialize_schemas=False).analysis,
    )

    assert [step.name for step in scalar_plan.steps] == ["project"]
    scalar_body = cast(PySparkStepBody, scalar_plan.steps[0].plugin_body)
    assert [operation.kind for operation in scalar_body.operations] == ["filter", "filter"]
    assert [assignment.expression.kind for assignment in scalar_body.projection[1:10]] == [
        "and",
        "contains",
        "like",
        "ilike",
        "rlike",
        "get_field",
        "cast",
        "try_cast",
        "call",
    ]
    assert scalar_body.projection[-1].expression.data is not None
    assert scalar_body.projection[-1].expression.data["function"] == "window_row_number"
    assert scalar_body.projection[-1].expression.data["order_count"] == 2

    collection = cast(PySparkStepBody, analytics_plan.steps[-1].plugin_body)
    functions = [
        assignment.expression.data["function"]
        for assignment in collection.projection
        if assignment.expression.data is not None and assignment.expression.kind == "transform_expression"
    ]
    assert {
        "collection_size",
        "array_contains",
        "array_repeat",
        "array_union",
        "array_except",
        "element_at",
        "try_element_at",
        "map_concat",
    } <= set(functions)


def test_v3_model_includes_checked_in_generated_docs_and_pyspark() -> None:
    root = Path(__file__).resolve().parents[3] / "res/testing/model/v3/structure_generated/orders"
    generated = (root / "pyspark/transforms/v3.py").read_text(encoding="utf-8")
    index = (root / "docs/index.md").read_text(encoding="utf-8")

    assert "class V3OrderFeaturesGenerated" in generated
    assert ".try_cast('int')" in generated
    assert "V3OrderFeatures" in index


def test_v3_orders_fixture_includes_generated_method_layout_matrix() -> None:
    root = Path(__file__).resolve().parents[3] / "res/testing/model/v3/structure_generated/orders/pyspark"
    variants = {
        "transforms": (),
        "transforms-mirror": ("def normalize(self):", "self._impl = EnrichOrders()"),
        "transforms-mirror-embed": ("def normalize(self):", "def remove_negative_totals(self, *, orders, spark, ctx):"),
    }

    for directory, expected in variants.items():
        text = (root / directory / "order.py").read_text(encoding="utf-8")
        parse(text)
        assert all(fragment in text for fragment in expected)


def _stub_pyspark(monkeypatch: pytest.MonkeyPatch) -> None:
    pyspark = python_types.ModuleType("pyspark")
    sql = python_types.ModuleType("pyspark.sql")
    functions = python_types.ModuleType("pyspark.sql.functions")

    class StorageLevel:
        MEMORY_AND_DISK: "StorageLevel"

        def __init__(self, use_disk, use_memory, use_off_heap, deserialized, replication=1):
            self.useDisk = use_disk
            self.useMemory = use_memory
            self.useOffHeap = use_off_heap
            self.deserialized = deserialized
            self.replication = replication

    StorageLevel.MEMORY_AND_DISK = StorageLevel(True, True, False, False)

    def expression_function(*args: object, **kwargs: object) -> object:
        return object()

    setattr(functions, "col", expression_function)
    setattr(functions, "lit", expression_function)
    setattr(pyspark, "StorageLevel", StorageLevel)
    setattr(sql, "functions", functions)
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
