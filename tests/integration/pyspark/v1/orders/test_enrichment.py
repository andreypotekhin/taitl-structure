from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from integration.pyspark.support.backend_matrix import (
    assert_generated_connect_safe,
    generated_project,
    render_generated_project,
)
from integration.pyspark.support.rows import rows
from integration.pyspark.v1.support import orders

pytestmark = pytest.mark.integration


def test_online_and_generated_execution_match_orders_contract_on_live_backend(spark, tmp_path) -> None:
    generated_package = "integration_v1_generated"
    files = render_generated_project(
        orders.EnrichOrders,
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        generated_package=generated_package,
        source_schema_modules=orders.source_schema_modules(),
    )

    with generated_project(tmp_path, generated_package, files):
        schemas = orders.generated_order_schemas(generated_package)
        generated = orders.run_generated_transform(spark, generated_package, schemas)
        online = orders.run_online_transform(spark, schemas)

        assert generated.columns == schemas.ORDER_PUBLISHED_SCHEMA.fieldNames()
        assert online.columns == schemas.ORDER_PUBLISHED_SCHEMA.fieldNames()
        generated_rows = rows(generated, "id")
        online_rows = rows(online, "id")
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
                "is_large": True,
                "has_promotion": True,
            }
        ]

    assert_generated_connect_safe(files)
