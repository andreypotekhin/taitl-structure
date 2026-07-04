import importlib
import sys
import types
from typing import Any, cast

import pytest

from structure import (
    JoinStrategy,
    Long,
    String,
    Structure,
    StructureCompileError,
    TiePolicy,
    Transform,
    count,
    field,
    group_by,
    input,
    output,
    sum,
    transform,
)
from structure.app.compiler.api import OperationCardinality
from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.dsl.api import compile_transform
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


def test_v2_order_fixture_records_supported_existence_joins(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pyspark(monkeypatch)
    module = importlib.import_module("testing.model.v2.orders.transforms.order")
    transform = cast(Any, module).EnrichOrders

    plan = compile_transform(transform)
    add_product = next(step for step in plan.steps if step.name == "add_product")

    assert [join.method for join in add_product.joins[:2]] == [JoinMethod.EXISTS, JoinMethod.NOT_EXISTS]
    assert [operation.capability.name for operation in add_product.operations[:2] if operation.capability] == [
        "exists",
        "not_exists",
    ]
    assert [operation.cardinality for operation in add_product.operations[:2]] == [
        OperationCardinality.ROW_FILTERING,
        OperationCardinality.ROW_FILTERING,
    ]


def test_v2_order_fixture_records_temporal_promotion_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pyspark(monkeypatch)
    module = importlib.import_module("testing.model.v2.orders.transforms.order")
    transform = cast(Any, module).EnrichOrders

    plan = compile_transform(transform)
    add_promotion = next(step for step in plan.steps if step.name == "add_promotion")
    lookup = add_promotion.joins[0]

    assert lookup.method is JoinMethod.TEMPORAL_ONE
    assert lookup.temporal is not None
    assert lookup.temporal.valid_from.data is not None
    assert lookup.temporal.valid_from.data["field"] == "valid_from"
    assert lookup.temporal.valid_to.data is not None
    assert lookup.temporal.valid_to.data["field"] == "valid_to"
    operation = next(operation for operation in add_promotion.operations if operation.kind == "join")
    assert operation.capability is not None
    assert operation.capability.name == "temporal_one"
    assert operation.cardinality is OperationCardinality.SELECT_ONE


def test_v2_order_fixture_records_cache_as_transform_option(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pyspark(monkeypatch)
    module = importlib.import_module("testing.model.v2.orders.transforms.order")
    transform = cast(Any, module).EnrichOrders

    plan = compile_transform(transform)
    add_customer = next(step for step in plan.steps if step.name == "add_customer")
    operation = next(operation for operation in add_customer.operations if operation.kind == "cache")

    assert operation.capability is not None
    assert operation.capability.group == "optimization"
    assert operation.capability.name == "cache"
    assert operation.cardinality is OperationCardinality.ROW_PRESERVING


def test_v2_order_fixture_records_join_many_shipments(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pyspark(monkeypatch)
    module = importlib.import_module("testing.model.v2.orders.transforms.order")
    transform = cast(Any, module).EnrichOrders

    plan = compile_transform(transform)
    add_shipments = next(step for step in plan.steps if step.name == "add_shipments")

    assert len(add_shipments.joins) == 1
    assert add_shipments.joins[0].method is JoinMethod.MANY
    assert add_shipments.joins[0].input_name == "shipment"
    assert add_shipments.joins[0].strategy is JoinStrategy.SHUFFLE_HASH
    assert add_shipments.operations[0].capability is not None
    assert add_shipments.operations[0].capability.name == "join_many"
    assert add_shipments.operations[0].cardinality is OperationCardinality.ROW_MULTIPLYING


def test_v2_order_fixture_records_deduped_product_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pyspark(monkeypatch)
    module = importlib.import_module("testing.model.v2.orders.transforms.order")
    transform = cast(Any, module).EnrichOrders

    plan = compile_transform(transform)
    add_product = next(step for step in plan.steps if step.name == "add_product")
    lookup = add_product.joins[2]

    assert lookup.method is JoinMethod.ONE
    assert lookup.dedupe is not None
    assert lookup.dedupe.direction == "latest"
    assert lookup.dedupe.ties is TiePolicy.ERROR
    assert lookup.dedupe.order_by.data is not None
    assert lookup.dedupe.order_by.data["field"] == "audit.ingested_at"


def test_group_by_lowers_to_aggregate_recipe() -> None:
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

    plan = PySpark.plan.lower()(compile_transform(Totals))

    operation = plan.steps[0].operations[0]
    assert operation.kind == "aggregate"
    assert operation.aggregate is not None
    assert [key.name for key in operation.aggregate.keys] == ["customer_id"]
    assert [(item.field.name, item.function) for item in operation.aggregate.assignments] == [
        ("customer_id", "key"),
        ("quantity", "count"),
    ]


def test_aggregate_expression_without_group_by_fails_in_frontend() -> None:
    class Raw(Structure):
        customer_id = field(String(), nullable=False)

    class Total(Structure):
        customer_id = field(String(), nullable=False)
        quantity = field(Long(), nullable=False)

    @transform
    class Totals(Transform):
        rows = input(Raw)
        totals = output(Total)

        def total(self, row: Raw) -> Total:
            return Total(customer_id=row.customer_id, quantity=count())

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Totals)

    assert raised.value.diagnostic.code == "DSL-E0402"
    assert "outside group_by" in raised.value.diagnostic.problem


def test_numeric_aggregate_rejects_non_numeric_input_type() -> None:
    class Raw(Structure):
        customer_id = field(String(), nullable=False)
        label = field(String(), nullable=False)

    class Total(Structure):
        customer_id = field(String(), nullable=False)
        label_total = field(String(), nullable=False)

    @transform
    class Totals(Transform):
        rows = input(Raw)
        totals = output(Total)

        def total(self, row: Raw) -> Total:
            group_by(customer_id=row.customer_id)
            return Total(customer_id=row.customer_id, label_total=sum(row.label))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Totals)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "DSL-E0402"
    assert "sum(...)" in diagnostic.problem
    assert "numeric expression" in diagnostic.use


def test_nullable_aggregate_input_cannot_feed_non_nullable_output() -> None:
    class Raw(Structure):
        customer_id = field(String(), nullable=False)
        quantity = field(Long(), nullable=True)

    class Total(Structure):
        customer_id = field(String(), nullable=False)
        quantity = field(Long(), nullable=False)

    @transform
    class Totals(Transform):
        rows = input(Raw)
        totals = output(Total)

        def total(self, row: Raw) -> Total:
            group_by(customer_id=row.customer_id)
            return Total(customer_id=row.customer_id, quantity=sum(row.quantity))

    with pytest.raises(StructureCompileError) as raised:
        compile_transform(Totals)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "SCHEMA-E0301"
    assert "may produce null" in diagnostic.problem


def test_v2_order_analytics_fixture_lowers_grouped_aggregates(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_pyspark(monkeypatch)
    module = importlib.import_module("testing.model.v2.orders.transforms.analytics")

    plan = PySpark.plan.lower()(compile_transform(module.OrderAnalytics))

    assert [step.name for step in plan.steps] == [
        "customer_daily_totals",
        "product_daily_summary",
        "customer_event_ranks",
    ]
    assert [step.operations[0].kind for step in plan.steps[:2]] == ["aggregate", "aggregate"]
    aggregates = tuple(step.operations[0].aggregate for step in plan.steps[:2])
    assert all(aggregate is not None for aggregate in aggregates)
    assert [
        [(assignment.field.name, assignment.function) for assignment in aggregate.assignments]
        for aggregate in aggregates
        if aggregate is not None
    ] == [
        [
            ("tenant", "first"),
            ("customer_id", "key"),
            ("order_date", "key"),
            ("order_count", "count"),
            ("gross_total", "sum"),
            ("net_total", "sum"),
        ],
        [
            ("tenant", "first"),
            ("product_id", "key"),
            ("order_date", "key"),
            ("order_count", "count"),
            ("distinct_customers", "count_distinct"),
            ("units", "sum"),
            ("min_units", "min"),
            ("max_units", "max"),
            ("avg_units", "avg"),
            ("gross_total", "sum"),
        ],
    ]
    projection = plan.steps[2].projection
    assert [(assignment.field.name, assignment.expression.data["function"]) for assignment in projection[4:]] == [
        ("row_number", "window_row_number"),
        ("rank", "window_rank"),
        ("dense_rank", "window_dense_rank"),
        ("previous_sequence", "window_lag"),
        ("next_sequence", "window_lead"),
    ]


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
