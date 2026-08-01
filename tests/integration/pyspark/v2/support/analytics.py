from __future__ import annotations

import importlib
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

from integration.pyspark.v2.support._common import transform_type


def transform():
    return transform_type("testing.model.orders.transforms.analytics", "OrderAnalytics")


def source_schema_modules():
    from testing.model.orders.schemas.analytics import CustomerDailyTotal, CustomerEventRank, ProductDailySummary
    from testing.model.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
    from testing.model.orders.schemas.order import (
        OrderFulfillment,
        OrderNormalized,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
    )

    return {
        "testing.model.orders.schemas.analytics": [
            CustomerDailyTotal,
            CustomerEventRank,
            ProductDailySummary,
        ],
        "testing.model.orders.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
        "testing.model.orders.schemas.order": [
            OrderNormalized,
            OrderWithCustomer,
            OrderWithProduct,
            OrderWithPromotion,
            OrderFulfillment,
        ],
    }


def generated_schemas(package: str):
    analytics = importlib.import_module(f"{package}.pyspark.schemas.analytics")
    order = importlib.import_module(f"{package}.pyspark.schemas.order")
    return SimpleNamespace(
        CUSTOMER_DAILY_TOTAL_SCHEMA=analytics.CUSTOMER_DAILY_TOTAL_SCHEMA,
        CUSTOMER_EVENT_RANK_SCHEMA=analytics.CUSTOMER_EVENT_RANK_SCHEMA,
        PRODUCT_DAILY_SUMMARY_SCHEMA=analytics.PRODUCT_DAILY_SUMMARY_SCHEMA,
        ORDER_FULFILLMENT_SCHEMA=order.ORDER_FULFILLMENT_SCHEMA,
    )


def fulfilled_frame(spark, schemas):
    return spark.createDataFrame(fulfilled_rows(), schema=schemas.ORDER_FULFILLMENT_SCHEMA)


def fulfilled_rows() -> list[dict[str, object]]:
    base = {
        "tenant": {"tenant_id": "t1"},
        "audit": {"source_system": "test", "ingested_at": datetime(2026, 1, 2, 7, 0)},
        "business": {"order_date": date(2026, 1, 2)},
        "promotion_code": None,
        "discount": Decimal("10.00"),
        "tags": [],
        "attributes": {},
        "shipping": None,
        "is_large": False,
        "customer_name": None,
        "customer_tier": None,
        "customer_region": None,
        "product_name": None,
        "product_category": None,
        "product_active": True,
        "product_list_price": Decimal("0.00"),
        "promotion_name": None,
        "promotion_discount": None,
        "shipment_line": 1,
        "carrier": "ups",
        "tracking_number": "track",
        "shipped_at": datetime(2026, 1, 3, 8, 30),
    }
    return [
        dict(
            base,
            id="o-1",
            customer_id="c-1",
            product_id="p-1",
            total=Decimal("100.00"),
            net_total=Decimal("90.00"),
            quantity=2,
        ),
        dict(
            base,
            id="o-2",
            customer_id="c-2",
            product_id="p-1",
            total=Decimal("200.00"),
            net_total=Decimal("190.00"),
            quantity=4,
        ),
        dict(
            base,
            id="o-3",
            customer_id="c-1",
            product_id="p-2",
            total=Decimal("300.00"),
            net_total=Decimal("280.00"),
            quantity=5,
        ),
    ]
