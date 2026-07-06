from __future__ import annotations

import importlib
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest
from integration.pyspark.matrix_support import (
    assert_generated_connect_safe,
    generated_project,
    render_generated_project,
    session,
)

pytestmark = pytest.mark.integration


def test_v2_online_and_generated_execution_match_order_enrichment_on_live_backend(spark, tmp_path) -> None:
    EnrichOrders = _transform_type("testing.model.v2.orders.transforms.order", "EnrichOrders")
    generated_package = "integration_v2_orders_generated"
    files = render_generated_project(
        EnrichOrders,
        source_transform="testing.model.v2.orders.transforms.order.EnrichOrders",
        generated_package=generated_package,
        source_schema_modules=_order_schema_modules(),
    )

    with generated_project(tmp_path, generated_package, files):
        schemas = _generated_order_schemas(generated_package)
        generated = _run_generated_orders_transform(spark, generated_package, schemas)
        online = _run_online_orders_transform(spark, schemas)

        assert generated.columns == schemas.ORDER_PUBLISHED_SCHEMA.fieldNames()
        assert online.columns == schemas.ORDER_PUBLISHED_SCHEMA.fieldNames()
        generated_rows = _rows(generated, "id")
        online_rows = _rows(online, "id")
        assert online_rows == generated_rows
        assert generated_rows == [
            {
                "tenant": {"tenant_id": "t1"},
                "business": {"order_date": date(2026, 1, 2)},
                "id": "o-1",
                "customer_id": "c-1",
                "customer_name": "Ada Lovelace",
                "customer_tier": "gold",
                "product_name": "Analytical Engine",
                "product_category": "compute",
                "promotion_name": "Summer",
                "total": Decimal("1250.50"),
                "discount": Decimal("10.00"),
                "net_total": Decimal("1240.50"),
                "quantity": 2,
                "carrier": "ups",
                "tracking_number": "1Z999",
                "shipped_at": datetime(2026, 1, 3, 8, 30),
                "is_large": True,
                "has_promotion": True,
            }
        ]

    assert_generated_connect_safe(files)


def test_v2_online_and_generated_execution_match_analytics_on_live_backend(spark, tmp_path) -> None:
    OrderAnalytics = _transform_type("testing.model.v2.orders.transforms.analytics", "OrderAnalytics")
    generated_package = "integration_v2_analytics_generated"
    files = render_generated_project(
        OrderAnalytics,
        source_transform="testing.model.v2.orders.transforms.analytics.OrderAnalytics",
        generated_package=generated_package,
        source_schema_modules=_analytics_schema_modules(),
    )

    with generated_project(tmp_path, generated_package, files):
        schemas = _generated_analytics_schemas(generated_package)
        frame = spark.createDataFrame(_fulfilled_rows(), schema=schemas.ORDER_FULFILLMENT_SCHEMA)
        generated = OrderAnalytics(fulfilled=frame).run(
            session(spark, execution_mode="generated", generated_package=generated_package)
        )
        online = OrderAnalytics(fulfilled=frame).run(session(spark, execution_mode="online"))

        assert _rows(online.customer_totals, "customer_id") == _rows(generated.customer_totals, "customer_id")
        assert _rows(online.product_summary, "product_id") == _rows(generated.product_summary, "product_id")
        assert _rows(online.customer_event_rank, "customer_id") == _rows(
            generated.customer_event_rank, "customer_id"
        )
        assert _rows(generated.product_summary, "product_id") == [
            {
                "tenant": {"tenant_id": "t1"},
                "product_id": "p-1",
                "order_date": date(2026, 1, 2),
                "order_count": 2,
                "distinct_customers": 2,
                "units": 6,
                "min_units": 2,
                "max_units": 4,
                "avg_units": 3.0,
                "gross_total": Decimal("300.00"),
            },
            {
                "tenant": {"tenant_id": "t1"},
                "product_id": "p-2",
                "order_date": date(2026, 1, 2),
                "order_count": 1,
                "distinct_customers": 1,
                "units": 5,
                "min_units": 5,
                "max_units": 5,
                "avg_units": 5.0,
                "gross_total": Decimal("300.00"),
            },
        ]

    assert_generated_connect_safe(files)


def test_v2_online_and_generated_execution_match_rowset_joins_on_live_backend(spark, tmp_path) -> None:
    RowsetJoinExamples = _transform_type("testing.model.v2.orders.transforms.rowset_join", "RowsetJoinExamples")
    generated_package = "integration_v2_rowset_generated"
    files = render_generated_project(
        RowsetJoinExamples,
        source_transform="testing.model.v2.orders.transforms.rowset_join.RowsetJoinExamples",
        generated_package=generated_package,
        source_schema_modules=_rowset_schema_modules(),
    )

    with generated_project(tmp_path, generated_package, files):
        schemas = _generated_rowset_schemas(generated_package)
        inputs = _rowset_input_frames(spark, schemas)
        generated = RowsetJoinExamples(**inputs).run(
            session(spark, execution_mode="generated", generated_package=generated_package)
        )
        online = RowsetJoinExamples(**inputs).run(session(spark, execution_mode="online"))

        generated_rows = _rows(generated.candidates, "customer_id", "product_id")
        assert _rows(online.candidates, "customer_id", "product_id") == generated_rows
        assert generated_rows == [
            {
                "tenant_id": "t1",
                "order_id": "o-1",
                "customer_id": "c-1",
                "customer_name": "Ada Lovelace",
                "product_id": "p-1",
                "product_name": "Analytical Engine",
            },
            {
                "tenant_id": "t1",
                "order_id": "o-1",
                "customer_id": "c-1",
                "customer_name": "Ada Lovelace",
                "product_id": "p-2",
                "product_name": "Compiler",
            },
            {
                "tenant_id": "t1",
                "order_id": None,
                "customer_id": "c-2",
                "customer_name": "Grace Hopper",
                "product_id": "p-1",
                "product_name": "Analytical Engine",
            },
            {
                "tenant_id": "t1",
                "order_id": None,
                "customer_id": "c-2",
                "customer_name": "Grace Hopper",
                "product_id": "p-2",
                "product_name": "Compiler",
            },
        ]

    assert_generated_connect_safe(files)


def _order_schema_modules():
    from testing.model.v2.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
    from testing.model.v2.orders.schemas.customer import Customer
    from testing.model.v2.orders.schemas.order import (
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
    from testing.model.v2.orders.schemas.product import BlockedProduct, Product, ProductBase
    from testing.model.v2.orders.schemas.promotion import Promotion
    from testing.model.v2.orders.schemas.shipment import Shipment

    return {
        "testing.model.v2.orders.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
        "testing.model.v2.orders.schemas.customer": [Customer],
        "testing.model.v2.orders.schemas.order": [
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
        "testing.model.v2.orders.schemas.product": [ProductBase, Product, BlockedProduct],
        "testing.model.v2.orders.schemas.promotion": [Promotion],
        "testing.model.v2.orders.schemas.shipment": [Shipment],
    }


def _rowset_schema_modules():
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


def _analytics_schema_modules():
    from testing.model.v2.orders.schemas.analytics import CustomerDailyTotal, CustomerEventRank, ProductDailySummary
    from testing.model.v2.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
    from testing.model.v2.orders.schemas.order import (
        OrderFulfillment,
        OrderNormalized,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
    )

    return {
        "testing.model.v2.orders.schemas.analytics": [
            CustomerDailyTotal,
            CustomerEventRank,
            ProductDailySummary,
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


def _generated_order_schemas(package: str):
    order = importlib.import_module(f"{package}.pyspark.schemas.order")
    customer = importlib.import_module(f"{package}.pyspark.schemas.customer")
    product = importlib.import_module(f"{package}.pyspark.schemas.product")
    promotion = importlib.import_module(f"{package}.pyspark.schemas.promotion")
    shipment = importlib.import_module(f"{package}.pyspark.schemas.shipment")
    return _Schemas(
        ORDER_RAW_SCHEMA=order.ORDER_RAW_SCHEMA,
        ORDER_PUBLISHED_SCHEMA=order.ORDER_PUBLISHED_SCHEMA,
        CUSTOMER_SCHEMA=customer.CUSTOMER_SCHEMA,
        PRODUCT_SCHEMA=product.PRODUCT_SCHEMA,
        BLOCKED_PRODUCT_SCHEMA=product.BLOCKED_PRODUCT_SCHEMA,
        PROMOTION_SCHEMA=promotion.PROMOTION_SCHEMA,
        SHIPMENT_SCHEMA=shipment.SHIPMENT_SCHEMA,
    )


def _generated_analytics_schemas(package: str):
    analytics = importlib.import_module(f"{package}.pyspark.schemas.analytics")
    order = importlib.import_module(f"{package}.pyspark.schemas.order")
    return _Schemas(
        CUSTOMER_DAILY_TOTAL_SCHEMA=analytics.CUSTOMER_DAILY_TOTAL_SCHEMA,
        CUSTOMER_EVENT_RANK_SCHEMA=analytics.CUSTOMER_EVENT_RANK_SCHEMA,
        PRODUCT_DAILY_SUMMARY_SCHEMA=analytics.PRODUCT_DAILY_SUMMARY_SCHEMA,
        ORDER_FULFILLMENT_SCHEMA=order.ORDER_FULFILLMENT_SCHEMA,
    )


def _generated_rowset_schemas(package: str):
    order = importlib.import_module(f"{package}.pyspark.schemas.order")
    customer = importlib.import_module(f"{package}.pyspark.schemas.customer")
    product = importlib.import_module(f"{package}.pyspark.schemas.product")
    return _Schemas(
        ORDER_RAW_SCHEMA=order.ORDER_RAW_SCHEMA,
        CUSTOMER_SCHEMA=customer.CUSTOMER_SCHEMA,
        PRODUCT_SCHEMA=product.PRODUCT_SCHEMA,
    )


def _run_generated_orders_transform(spark, generated_package: str, schemas):
    EnrichOrders = _transform_type("testing.model.v2.orders.transforms.order", "EnrichOrders")
    invocation = EnrichOrders(**_order_input_frames(spark, schemas))
    return invocation.run(session(spark, execution_mode="generated", generated_package=generated_package)).published


def _run_online_orders_transform(spark, schemas):
    EnrichOrders = _transform_type("testing.model.v2.orders.transforms.order", "EnrichOrders")
    return EnrichOrders(**_order_input_frames(spark, schemas)).run(session(spark, execution_mode="online")).published


def _order_input_frames(spark, schemas) -> dict[str, object]:
    return {
        "orders": spark.createDataFrame(_raw_order_rows(), schema=schemas.ORDER_RAW_SCHEMA),
        "customers": spark.createDataFrame(_customer_rows(), schema=schemas.CUSTOMER_SCHEMA),
        "products": spark.createDataFrame(_product_rows(), schema=schemas.PRODUCT_SCHEMA),
        "blocked_products": spark.createDataFrame([], schema=schemas.BLOCKED_PRODUCT_SCHEMA),
        "promotions": spark.createDataFrame(_promotion_rows(), schema=schemas.PROMOTION_SCHEMA),
        "shipments": spark.createDataFrame(_shipment_rows(), schema=schemas.SHIPMENT_SCHEMA),
    }


def _rowset_input_frames(spark, schemas) -> dict[str, object]:
    return {
        "orders": spark.createDataFrame(_rowset_order_rows(), schema=schemas.ORDER_RAW_SCHEMA),
        "customers": spark.createDataFrame(_rowset_customer_rows(), schema=schemas.CUSTOMER_SCHEMA),
        "products": spark.createDataFrame(_rowset_product_rows(), schema=schemas.PRODUCT_SCHEMA),
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


def _rowset_order_rows() -> list[dict[str, object]]:
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
        }
    ]


def _rowset_customer_rows() -> list[dict[str, object]]:
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
        }
    ]


def _rowset_product_rows() -> list[dict[str, object]]:
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


def _fulfilled_rows() -> list[dict[str, object]]:
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


def _rows(frame, *order_by: str) -> list[dict[str, object]]:
    return [row.asDict(recursive=True) for row in frame.orderBy(*order_by).collect()]


def _transform_type(module: str, name: str) -> Any:
    return getattr(importlib.import_module(module), name)


class _Schemas:
    def __init__(self, **values) -> None:
        self.__dict__.update(values)
