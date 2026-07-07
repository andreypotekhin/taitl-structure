from __future__ import annotations

import importlib
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

from integration.pyspark.v2.support._common import transform_type


def transform():
    return transform_type("testing.model.v2.orders.transforms.rowset_join", "RowsetJoinExamples")


def source_schema_modules():
    from testing.model.v2.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
    from testing.model.v2.orders.schemas.customer import Customer
    from testing.model.v2.orders.schemas.order import (
        CustomerOrderBackfill,
        OrderCustomerReconciliation,
        OrderProductCandidate,
        OrderRaw,
    )
    from testing.model.v2.orders.schemas.product import Product, ProductBase

    return {
        "testing.model.v2.orders.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
        "testing.model.v2.orders.schemas.customer": [Customer],
        "testing.model.v2.orders.schemas.order": [
            OrderRaw,
            OrderCustomerReconciliation,
            CustomerOrderBackfill,
            OrderProductCandidate,
        ],
        "testing.model.v2.orders.schemas.product": [ProductBase, Product],
    }


def generated_schemas(package: str):
    order = importlib.import_module(f"{package}.pyspark.schemas.order")
    customer = importlib.import_module(f"{package}.pyspark.schemas.customer")
    product = importlib.import_module(f"{package}.pyspark.schemas.product")
    return SimpleNamespace(
        ORDER_RAW_SCHEMA=order.ORDER_RAW_SCHEMA,
        CUSTOMER_SCHEMA=customer.CUSTOMER_SCHEMA,
        PRODUCT_SCHEMA=product.PRODUCT_SCHEMA,
    )


def input_frames(spark, schemas) -> dict[str, object]:
    return {
        "orders": spark.createDataFrame(_order_rows(), schema=schemas.ORDER_RAW_SCHEMA),
        "customers": spark.createDataFrame(_customer_rows(), schema=schemas.CUSTOMER_SCHEMA),
        "products": spark.createDataFrame(_product_rows(), schema=schemas.PRODUCT_SCHEMA),
    }


def _order_rows() -> list[dict[str, object]]:
    return [
        {
            "tenant": {"tenant_id": "t1"},
            "audit": {"source_system": "web", "ingested_at": datetime(2026, 1, 2, 7, 0)},
            "business": {"order_date": date(2026, 1, 2)},
            "id": "o-1",
            "customer_id": "c-1",
            "product_id": "p-1",
            "promo-code": None,
            "total": "1250.50",
            "discount": "10.00",
            "quantity": 2,
            "tags": ["new", "priority"],
            "attributes": {"channel": "web"},
            "shipping": None,
        }
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
        },
        {
            "tenant": {"tenant_id": "t1"},
            "audit": {"source_system": "crm", "ingested_at": datetime(2026, 1, 2, 6, 5)},
            "id": "c-2",
            "name": "Grace Hopper",
            "tier": "silver",
            "region": "east",
            "email": "grace@example.test",
        },
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
        },
        {
            "tenant": {"tenant_id": "t1"},
            "audit": {"source_system": "catalog", "ingested_at": datetime(2026, 1, 2, 6, 35)},
            "id": "p-2",
            "name": "Compiler",
            "category": "software",
            "active": True,
            "list_price": Decimal("300.00"),
            "weight": 0.0,
            "rating": 4.5,
        },
    ]
