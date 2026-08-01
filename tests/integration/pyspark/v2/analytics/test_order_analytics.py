from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from integration.pyspark.support.backend_matrix import (
    assert_generated_connect_safe,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.rows import rows
from integration.pyspark.v2.support import analytics

pytestmark = pytest.mark.integration


def test_online_and_generated_execution_match_analytics_on_live_backend(spark, tmp_path) -> None:
    OrderAnalytics = analytics.transform()
    generated_package = "integration_v2_analytics_generated"
    files = render_generated_project(
        OrderAnalytics,
        source_transform="testing.model.orders.transforms.analytics.OrderAnalytics",
        generated_package=generated_package,
        source_schema_modules=analytics.source_schema_modules(),
    )

    with generated_project(tmp_path, generated_package, files):
        schemas = analytics.generated_schemas(generated_package)
        frame = analytics.fulfilled_frame(spark, schemas)
        generated = OrderAnalytics(fulfilled=frame).run(
            session(spark, execution_mode="generated", generated_package=generated_package)
        )
        online = OrderAnalytics(fulfilled=frame).run(session(spark, execution_mode="online"))

        assert rows(online.customer_totals, "customer_id") == rows(generated.customer_totals, "customer_id")
        assert rows(online.product_summary, "product_id") == rows(generated.product_summary, "product_id")
        assert rows(online.customer_event_rank, "customer_id") == rows(generated.customer_event_rank, "customer_id")
        assert rows(generated.product_summary, "product_id") == [
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
