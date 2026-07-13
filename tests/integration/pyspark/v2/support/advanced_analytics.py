from __future__ import annotations

import csv
import importlib
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from integration.pyspark.v2.support._common import transform_type

ROOT = Path(__file__).resolve().parents[5]
DATA = ROOT / "res" / "testing" / "data" / "v2" / "orders"


def transform():
    return transform_type("testing.model.v2.orders.transforms.adv_analytics", "AdvancedOrderAnalytics")


def source_schema_modules():
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


def generated_schemas(package: str):
    advanced = importlib.import_module(f"{package}.pyspark.schemas.adv_analytics")
    order = importlib.import_module(f"{package}.pyspark.schemas.order")
    return SimpleNamespace(
        ORDER_COLLECTION_SOURCE_SCHEMA=advanced.ORDER_COLLECTION_SOURCE_SCHEMA,
        ORDER_FULFILLMENT_SCHEMA=order.ORDER_FULFILLMENT_SCHEMA,
    )


def input_frames(spark, schemas) -> dict[str, object]:
    return {
        "fulfilled": spark.createDataFrame(fulfilled_rows(), schema=schemas.ORDER_FULFILLMENT_SCHEMA),
        "collections": spark.createDataFrame(
            collection_rows(),
            schema=schemas.ORDER_COLLECTION_SOURCE_SCHEMA,
        ),
    }


def fulfilled_rows() -> list[dict[str, object]]:
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


def collection_rows() -> list[dict[str, object]]:
    return [
        {
            "id": row["id"],
            "tags": row["tags"].split("|"),
            "extra_tags": ["priority", "seasonal"],
            "nested_tags": [item.split("|") for item in row["nested_tags"].split(";")],
            "scores": [int(item) for item in row["scores"].split("|")],
            "attributes": dict(item.split("=", 1) for item in row["attributes"].split(";")),
            "extra_attributes": {"Region": "NA"},
        }
        for row in _csv("collections.csv")
    ]


def _csv(name: str) -> list[dict[str, str]]:
    with (DATA / name).open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))
