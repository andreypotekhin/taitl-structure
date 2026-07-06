from __future__ import annotations

import csv
import importlib
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from integration.pyspark.matrix_support import (
    assert_generated_connect_safe,
    generated_project,
    render_generated_project,
    session,
)

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "res" / "testing" / "data" / "v2" / "orders"


def test_v2_online_and_generated_execution_match_advanced_analytics_on_live_backend(spark, tmp_path) -> None:
    AdvancedOrderAnalytics = _transform_type(
        "testing.model.v2.orders.transforms.adv_analytics",
        "AdvancedOrderAnalytics",
    )
    generated_package = "integration_v2_adv_analytics_generated"
    files = render_generated_project(
        AdvancedOrderAnalytics,
        source_transform="testing.model.v2.orders.transforms.adv_analytics.AdvancedOrderAnalytics",
        generated_package=generated_package,
        source_schema_modules=_adv_analytics_schema_modules(),
    )

    with generated_project(tmp_path, generated_package, files):
        schemas = _generated_adv_analytics_schemas(generated_package)
        inputs = {
            "fulfilled": spark.createDataFrame(_fulfilled_rows(), schema=schemas.ORDER_FULFILLMENT_SCHEMA),
            "collections": spark.createDataFrame(
                _collection_rows(),
                schema=schemas.ORDER_COLLECTION_SOURCE_SCHEMA,
            ),
        }
        generated = AdvancedOrderAnalytics(**inputs).run(
            session(spark, execution_mode="generated", generated_package=generated_package)
        )
        online = AdvancedOrderAnalytics(**inputs).run(session(spark, execution_mode="online"))

        assert _sorted_rows(online.revenue_rollups) == _sorted_rows(generated.revenue_rollups)
        assert _sorted_rows(online.product_cubes) == _sorted_rows(generated.product_cubes)
        assert _rows(online.customer_windows, "customer_id", "quantity") == _rows(
            generated.customer_windows,
            "customer_id",
            "quantity",
        )
        assert _rows(online.collection_profiles, "id") == _rows(generated.collection_profiles, "id")

        grand_total = _single(
            generated.revenue_rollups,
            lambda row: row["tenant_id"] is None and row["product_category"] is None and row["order_date"] is None,
        )
        assert grand_total["order_count"] == 4
        assert grand_total["large_order_count"] == 1
        assert grand_total["quantity_total"] == 20
        assert grand_total["any_large_order"] is True
        assert grand_total["all_large_orders"] is False

        cube_total = _single(
            generated.product_cubes,
            lambda row: row["tenant_id"] is None and row["product_category"] is None and row["customer_tier"] is None,
        )
        assert cube_total["order_count"] == 4
        assert cube_total["distinct_customers"] == 3
        assert cube_total["gross_total"] == Decimal("2000.00")

        customer_second = _single(
            generated.customer_windows,
            lambda row: row["customer_id"] == "c-1" and row["order_id"] == "o-3",
        )
        assert customer_second["percent_rank"] == 1.0
        assert customer_second["cume_dist"] == 1.0
        assert customer_second["second_order_id"] == "o-3"
        assert customer_second["running_units"] == 7
        assert customer_second["running_order_count"] == 2

        profile = _single(generated.collection_profiles, lambda row: row["id"] == "o-1")
        assert profile["has_priority"] is True
        assert profile["all_tags_present"] is True
        assert profile["score_total"] == 6
        assert profile["flat_tags"] == ["priority", "new", "gift"]
        attribute_keys = profile["attribute_keys"]
        assert isinstance(attribute_keys, list)
        assert sorted(attribute_keys) == ["Campaign", "Channel"]
        assert profile["roundtrip_attributes"] == {"Channel": "WEB", "Campaign": "SUMMER"}

    assert_generated_connect_safe(files)


def _adv_analytics_schema_modules():
    from testing.model.v2.orders.schemas.adv_analytics import (
        OrderCollectionProfile,
        OrderCollectionSource,
        OrderCustomerWindow,
        OrderProductCube,
        OrderRevenueRollup,
    )
    from testing.model.v2.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
    from testing.model.v2.orders.schemas.order import (
        OrderFulfillment,
        OrderNormalized,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
    )

    return {
        "testing.model.v2.orders.schemas.adv_analytics": [
            OrderRevenueRollup,
            OrderProductCube,
            OrderCustomerWindow,
            OrderCollectionSource,
            OrderCollectionProfile,
        ],
        "testing.model.v2.orders.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
        "testing.model.v2.orders.schemas.order": [
            OrderNormalized,
            OrderWithCustomer,
            OrderWithProduct,
            OrderWithPromotion,
            OrderFulfillment,
        ],
    }


def _generated_adv_analytics_schemas(package: str):
    advanced = importlib.import_module(f"{package}.pyspark.schemas.adv_analytics")
    order = importlib.import_module(f"{package}.pyspark.schemas.order")
    return _Schemas(
        ORDER_COLLECTION_SOURCE_SCHEMA=advanced.ORDER_COLLECTION_SOURCE_SCHEMA,
        ORDER_FULFILLMENT_SCHEMA=order.ORDER_FULFILLMENT_SCHEMA,
    )


def _fulfilled_rows() -> list[dict[str, object]]:
    return [
        {
            "tenant": {"tenant_id": row["tenant_id"]},
            "audit": {
                "source_system": row["source_system"],
                "ingested_at": datetime.fromisoformat(row["ingested_at"]),
            },
            "business": {"order_date": date.fromisoformat(row["order_date"])},
            "id": row["id"],
            "customer_id": row["customer_id"],
            "product_id": row["product_id"],
            "promotion_code": None,
            "total": Decimal(row["total"]),
            "discount": Decimal("0.00"),
            "net_total": Decimal(row["net_total"]),
            "quantity": int(row["quantity"]),
            "tags": [],
            "attributes": {},
            "shipping": None,
            "is_large": row["is_large"] == "true",
            "customer_name": None,
            "customer_tier": row["customer_tier"],
            "customer_region": None,
            "product_name": None,
            "product_category": row["product_category"],
            "product_active": True,
            "product_list_price": Decimal(row["product_list_price"]),
            "promotion_name": None,
            "promotion_discount": None,
            "shipment_line": int(row["shipment_line"]),
            "carrier": row["carrier"],
            "tracking_number": row["tracking_number"],
            "shipped_at": datetime.fromisoformat(row["shipped_at"]),
        }
        for row in _csv("fulfilled.csv")
    ]


def _collection_rows() -> list[dict[str, object]]:
    return [
        {
            "id": row["id"],
            "tags": row["tags"].split("|"),
            "nested_tags": [item.split("|") for item in row["nested_tags"].split(";")],
            "scores": [int(item) for item in row["scores"].split("|")],
            "attributes": dict(item.split("=", 1) for item in row["attributes"].split(";")),
        }
        for row in _csv("collections.csv")
    ]


def _csv(name: str) -> list[dict[str, str]]:
    with (DATA / name).open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def _rows(frame, *order_by: str) -> list[dict[str, object]]:
    ordered = frame.orderBy(*order_by) if order_by else frame
    return [row.asDict(recursive=True) for row in ordered.collect()]


def _sorted_rows(frame) -> list[dict[str, object]]:
    return sorted(_rows(frame), key=repr)


def _single(frame, predicate) -> dict[str, object]:
    matches = [row for row in _rows(frame) if predicate(row)]
    assert len(matches) == 1
    return matches[0]


def _transform_type(module: str, name: str) -> Any:
    return getattr(importlib.import_module(module), name)


class _Schemas:
    def __init__(self, **values) -> None:
        self.__dict__.update(values)
