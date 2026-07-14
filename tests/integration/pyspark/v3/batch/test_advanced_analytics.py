from __future__ import annotations

import pytest
from integration.pyspark.support.backend_matrix import (
    assert_generated_connect_safe,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.rows import rows, single, sorted_rows
from integration.pyspark.v2.support import advanced_analytics

pytestmark = pytest.mark.integration


def test_v3_advanced_analytics_matches_generated_execution_on_live_backend(spark, tmp_path) -> None:
    AdvancedOrderAnalytics, source_schema_modules = _source()
    package = "integration_v3_advanced_analytics_generated"
    files = render_generated_project(
        AdvancedOrderAnalytics,
        source_transform="testing.model.v3.orders.transforms.adv_analytics.AdvancedOrderAnalytics",
        generated_package=package,
        source_schema_modules=source_schema_modules,
    )

    with generated_project(tmp_path, package, files):
        schemas = advanced_analytics.generated_schemas(package)
        inputs = advanced_analytics.input_frames(spark, schemas)
        generated = AdvancedOrderAnalytics(**inputs).run(
            session(spark, execution_mode="generated", generated_package=package)
        )
        online = AdvancedOrderAnalytics(**inputs).run(session(spark, execution_mode="online"))

        assert sorted_rows(online.revenue_rollups) == sorted_rows(generated.revenue_rollups)
        assert sorted_rows(online.product_cubes) == sorted_rows(generated.product_cubes)
        assert rows(online.customer_windows, "customer_id", "quantity") == rows(
            generated.customer_windows,
            "customer_id",
            "quantity",
        )
        assert rows(online.collection_profiles, "id") == rows(generated.collection_profiles, "id")

        grand_total = single(
            generated.revenue_rollups,
            lambda row: row["tenant_id"] is None and row["product_category"] is None and row["order_date"] is None,
        )
        assert grand_total["quantity_total"] == 20
        assert grand_total["any_large_order"] is True

        customer_second = single(
            generated.customer_windows,
            lambda row: row["customer_id"] == "c-1" and row["order_id"] == "o-3",
        )
        assert customer_second["running_units"] == 7

        profile = single(generated.collection_profiles, lambda row: row["id"] == "o-1")
        assert profile["roundtrip_attributes"] == {"Channel": "WEB", "Campaign": "SUMMER"}
        assert profile["merged_attributes"] == {"Channel": "WEB", "Campaign": "SUMMER", "Region": "NA"}
        assert profile["score_total"] == 6

    assert_generated_connect_safe(files)


def _source():
    from testing.model.v3.orders.schemas.adv_analytics import (
        OrderCollectionProfile,
        OrderCollectionSource,
        OrderCustomerWindow,
        OrderProductCube,
        OrderRevenueRollup,
    )
    from testing.model.v3.orders.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
    from testing.model.v3.orders.schemas.order import (
        OrderFulfillment,
        OrderNormalized,
        OrderWithCustomer,
        OrderWithProduct,
        OrderWithPromotion,
    )
    from testing.model.v3.orders.transforms.adv_analytics import AdvancedOrderAnalytics

    return AdvancedOrderAnalytics, {
        "testing.model.v3.orders.schemas.adv_analytics": [
            OrderRevenueRollup,
            OrderProductCube,
            OrderCustomerWindow,
            OrderCollectionSource,
            OrderCollectionProfile,
        ],
        "testing.model.v3.orders.schemas.common": [TenantKey, AuditStamp, Address, BusinessDate],
        "testing.model.v3.orders.schemas.order": [
            OrderNormalized,
            OrderWithCustomer,
            OrderWithProduct,
            OrderWithPromotion,
            OrderFulfillment,
        ],
    }
