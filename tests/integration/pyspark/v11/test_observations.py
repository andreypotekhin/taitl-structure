from __future__ import annotations

import importlib

import pytest
from integration.pyspark.support.backend_matrix import (
    backend_name,
    generated_project,
    render_generated_projects,
    session,
)

from structure import Schema, Transform, input, observation, output, transform
from structure.plugin.pyspark import array, collect_set, count, long, observe, string, sum, where

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        backend_name() not in {"pyspark41", "spark-connect41"},
        reason="Typed batch observations require exact PySpark 4.1 profiles",
    ),
]

SOURCE_MODULE = "integration.pyspark.v11.test_observations"
PACKAGE = "integration_v11_observations_generated"


class Orders(Schema):
    order_id = string(nullable=False)
    quantity = long(nullable=False)


class AcceptedOrders(Schema):
    order_id = string(nullable=False)
    quantity = long(nullable=False)


class OrderMetrics(Schema):
    rows = long(nullable=False)
    large_orders = long(nullable=False)
    total_quantity = long(nullable=True)
    order_ids = array(string(), contains_null=False, nullable=False)


@transform
class MeasureAcceptedOrders(Transform):
    orders = input(Orders)
    accepted = output(AcceptedOrders)
    quality = observation(OrderMetrics)

    def accept(self, order: Orders) -> AcceptedOrders:
        where(order.quantity >= 0)
        observe(
            OrderMetrics(
                rows=count(),
                large_orders=count(where=order.quantity > 2),
                total_quantity=sum(order.quantity),
                order_ids=collect_set(order.order_id),
            )
        )
        return AcceptedOrders.project(order)


@transform
class RepublishAcceptedOrders(Transform):
    accepted = input(AcceptedOrders)
    published = output(AcceptedOrders)

    def publish(self, order: AcceptedOrders) -> AcceptedOrders:
        return AcceptedOrders.project(order)


@transform
class RepublishAcceptedOrdersPipeline(Transform):
    orders = input(Orders)
    pipeline = MeasureAcceptedOrders(orders=orders).to(RepublishAcceptedOrders())


def _files():
    files = render_generated_projects(
        [
            (MeasureAcceptedOrders, f"{SOURCE_MODULE}.MeasureAcceptedOrders"),
            (RepublishAcceptedOrdersPipeline, f"{SOURCE_MODULE}.RepublishAcceptedOrdersPipeline"),
        ],
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [Orders, AcceptedOrders, OrderMetrics]},
    )
    source = files[f"{PACKAGE}/pyspark/transforms/integration/pyspark/v11/test_observations.py"]
    assert "Observation(" in source
    assert ".observe(" in source
    assert "ObservationResult" in source
    assert "from integration.pyspark.v11.test_observations import OrderMetrics" in source
    assert not any(token in source for token in ("SparkContext", "sparkContext", "_jdf", "_jvm", ".rdd"))
    return files


def test_typed_observation_matches_native_online_and_generated(spark, tmp_path) -> None:
    from pyspark.sql import Observation
    from pyspark.sql import functions as F

    source = spark.createDataFrame(
        [("a", -1), ("b", 2), ("c", 3)],
        "order_id string not null, quantity long not null",
    )
    native_handle = Observation("native_observation")
    native = source.where(F.col("quantity") >= 0).observe(
        native_handle,
        F.count(F.lit(1)).alias("rows"),
        F.count(F.when(F.col("quantity") > 2, F.lit(1))).alias("large_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.collect_set("order_id").alias("order_ids"),
    )
    projected_handle = Observation("projected_observation")
    native_projected = source.where(F.col("quantity") >= 0).observe(
        projected_handle,
        F.count(F.lit(1)).alias("rows"),
        F.count(F.when(F.col("quantity") > 2, F.lit(1))).alias("large_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.collect_set("order_id").alias("order_ids"),
    ).select("order_id", "quantity")
    files = _files()
    with generated_project(tmp_path, PACKAGE, files):
        importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_observations")
        importlib.import_module(f"{PACKAGE}.pyspark.transforms.integration.pyspark.v11.test_observations")
        online = MeasureAcceptedOrders(orders=source).run(session(spark, execution_mode="online"))
        generated = MeasureAcceptedOrders(orders=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        if backend_name() == "spark-connect41":
            with pytest.raises(RuntimeError, match="run an action"):
                online.quality.get()
        native_rows = native.collect()
        assert native_handle.get["rows"] == 2
        assert native_handle.get["large_orders"] == 1
        assert native_handle.get["total_quantity"] == 5
        assert set(native_handle.get["order_ids"]) == {"b", "c"}
        native_projected.collect()
        assert set(projected_handle.get["order_ids"]) == {"b", "c"}
        online_rows = online.accepted.collect()
        generated_rows = generated.accepted.collect()
        empty_source = spark.createDataFrame([], source.schema)
        empty_online = MeasureAcceptedOrders(orders=empty_source).run(session(spark, execution_mode="online"))
        empty_generated = MeasureAcceptedOrders(orders=empty_source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        empty_online.accepted.collect()
        empty_generated.accepted.collect()
        composed = RepublishAcceptedOrdersPipeline(orders=source).run(session(spark, execution_mode="online"))
        composed_generated = RepublishAcceptedOrdersPipeline(orders=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        composed.published.collect()
        composed_generated.published.collect()

    assert [(row.order_id, row.quantity) for row in online_rows] == [("b", 2), ("c", 3)]
    assert [(row.order_id, row.quantity) for row in generated_rows] == [("b", 2), ("c", 3)]
    assert [(row.order_id, row.quantity) for row in native_rows] == [("b", 2), ("c", 3)]
    assert online.schema["accepted"] == generated.schema["accepted"]
    assert online.quality.schema is OrderMetrics
    assert generated.quality.schema is OrderMetrics
    assert online.quality.get().rows == 2
    assert online.quality.get().large_orders == 1
    assert online.quality.get().total_quantity == 5
    assert set(online.quality.get().order_ids) == {"b", "c"}
    assert generated.quality.get().rows == 2
    assert generated.quality.get().large_orders == 1
    assert generated.quality.get().total_quantity == 5
    assert set(generated.quality.get().order_ids) == {"b", "c"}
    assert set(native_handle.get["order_ids"]) == {"b", "c"}
    assert empty_online.quality.get().rows == 0
    assert empty_online.quality.get().large_orders == 0
    assert empty_online.quality.get().total_quantity is None
    assert empty_online.quality.get().order_ids == []
    assert empty_generated.quality.get().rows == 0
    assert empty_generated.quality.get().large_orders == 0
    assert empty_generated.quality.get().total_quantity is None
    assert empty_generated.quality.get().order_ids == []
    assert composed.stages["measure_accepted_orders"].quality.get().rows == 2
    assert composed_generated.stages["measure_accepted_orders"].quality.get().rows == 2
