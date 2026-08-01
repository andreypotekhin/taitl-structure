from __future__ import annotations

import importlib
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

from integration.pyspark.support.backend_matrix import session
from integration.pyspark.v2.support._common import transform_type


def transform():
    return transform_type("testing.model.orders.transforms.order", "EnrichOrders")


def source_schema_modules():
    from testing.model.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
    from testing.model.orders.schemas.customer import Customer
    from testing.model.orders.schemas.order import (
        OrderFulfillment,
        OrderNormalized,
        OrderPublication,
        OrderPublished,
        OrderRaw,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
        PublicationFlags,
    )
    from testing.model.orders.schemas.product import BlockedProduct, Product, ProductBase
    from testing.model.orders.schemas.promotion import Promotion
    from testing.model.orders.schemas.shipment import Shipment

    return {
        "testing.model.orders.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
        "testing.model.orders.schemas.customer": [Customer],
        "testing.model.orders.schemas.order": [
            OrderRaw,
            OrderNormalized,
            OrderWithCustomer,
            OrderWithProduct,
            OrderWithPromotion,
            OrderFulfillment,
            OrderPublication,
            PublicationFlags,
            OrderPublished,
        ],
        "testing.model.orders.schemas.product": [ProductBase, Product, BlockedProduct],
        "testing.model.orders.schemas.promotion": [Promotion],
        "testing.model.orders.schemas.shipment": [Shipment],
    }


def generated_schemas(package: str):
    order = importlib.import_module(f"{package}.pyspark.schemas.order")
    customer = importlib.import_module(f"{package}.pyspark.schemas.customer")
    product = importlib.import_module(f"{package}.pyspark.schemas.product")
    promotion = importlib.import_module(f"{package}.pyspark.schemas.promotion")
    shipment = importlib.import_module(f"{package}.pyspark.schemas.shipment")
    return SimpleNamespace(
        ORDER_RAW_SCHEMA=order.ORDER_RAW_SCHEMA,
        ORDER_PUBLISHED_SCHEMA=order.ORDER_PUBLISHED_SCHEMA,
        CUSTOMER_SCHEMA=customer.CUSTOMER_SCHEMA,
        PRODUCT_SCHEMA=product.PRODUCT_SCHEMA,
        BLOCKED_PRODUCT_SCHEMA=product.BLOCKED_PRODUCT_SCHEMA,
        PROMOTION_SCHEMA=promotion.PROMOTION_SCHEMA,
        SHIPMENT_SCHEMA=shipment.SHIPMENT_SCHEMA,
    )


def run_generated_transform(spark, generated_package: str, schemas):
    invocation = transform()(**input_frames(spark, schemas))
    return invocation.run(session(spark, execution_mode="generated", generated_package=generated_package)).published


def run_online_transform(spark, schemas):
    return transform()(**input_frames(spark, schemas)).run(session(spark, execution_mode="online")).published


def input_frames(spark, schemas) -> dict[str, object]:
    return {
        "orders": spark.createDataFrame(_raw_order_rows(), schema=schemas.ORDER_RAW_SCHEMA),
        "customers": spark.createDataFrame(_customer_rows(), schema=schemas.CUSTOMER_SCHEMA),
        "products": spark.createDataFrame(_product_rows(), schema=schemas.PRODUCT_SCHEMA),
        "blocked_products": spark.createDataFrame([], schema=schemas.BLOCKED_PRODUCT_SCHEMA),
        "promotions": spark.createDataFrame(_promotion_rows(), schema=schemas.PROMOTION_SCHEMA),
        "shipments": spark.createDataFrame(_shipment_rows(), schema=schemas.SHIPMENT_SCHEMA),
    }


def _raw_order_rows() -> list[dict[str, object]]:
    return [
        {
            "tenant": {"tenant_id": "t1"},
            "audit": {"source_system": "web", "ingested_at": datetime(2026, 1, 2, 7, 0)},
            "business": {"order_date": date(2026, 1, 2)},
            "id": " O-1 ",
            "customer_id": " C-1 ",
            "product_id": " P-1 ",
            "promo-code": " SUMMER ",
            "total": "1250.50",
            "discount": "10.00",
            "quantity": 2,
            "tags": [" NEW ", "PRIORITY"],
            "attributes": {"channel": " WEB "},
            "shipping": None,
        },
        {
            "tenant": {"tenant_id": "t1"},
            "audit": {"source_system": "web", "ingested_at": datetime(2026, 1, 2, 7, 5)},
            "business": {"order_date": date(2026, 1, 2)},
            "id": "blocked",
            "customer_id": "c-1",
            "product_id": "missing",
            "promo-code": None,
            "total": "25.00",
            "discount": "0.00",
            "quantity": 1,
            "tags": [],
            "attributes": {},
            "shipping": None,
        },
    ]


def _customer_rows() -> list[dict[str, object]]:
    return [
        {
            "tenant": {"tenant_id": "t1"},
            "audit": {"source_system": "crm", "ingested_at": datetime(2026, 1, 2, 6, 0)},
            "id": "c-1",
            "name": "Ada Lovelace",
            "tier": "gold",
            "region": "west",
            "email": "ada@example.test",
        }
    ]


def _product_rows() -> list[dict[str, object]]:
    return [
        {
            "tenant": {"tenant_id": "t1"},
            "audit": {"source_system": "catalog", "ingested_at": datetime(2026, 1, 2, 6, 30)},
            "id": "p-1",
            "name": "Analytical Engine",
            "category": "compute",
            "active": True,
            "list_price": Decimal("1250.50"),
            "weight": 1.25,
            "rating": 4.75,
        }
    ]


def _promotion_rows() -> list[dict[str, object]]:
    return [
        {
            "tenant": {"tenant_id": "t1"},
            "audit": {"source_system": "campaign", "ingested_at": datetime(2026, 1, 2, 6, 45)},
            "code": "summer",
            "name": "Summer",
            "discount": Decimal("10.00"),
            "valid_from": date(2026, 1, 1),
            "valid_to": date(2026, 12, 31),
        }
    ]


def _shipment_rows() -> list[dict[str, object]]:
    return [
        {
            "tenant": {"tenant_id": "t1"},
            "audit": {"source_system": "wms", "ingested_at": datetime(2026, 1, 3, 8, 0)},
            "order_id": "o-1",
            "line_number": 1,
            "carrier": "ups",
            "tracking_number": "1Z999",
            "shipped_at": datetime(2026, 1, 3, 8, 30),
        }
    ]
