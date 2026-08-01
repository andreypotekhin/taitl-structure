from __future__ import annotations

from decimal import Decimal

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


def test_online_and_generated_execution_match_advanced_analytics_on_live_backend(spark, tmp_path) -> None:
    AdvancedOrderAnalytics = advanced_analytics.transform()
    generated_package = "integration_v2_adv_analytics_generated"
    files = render_generated_project(
        AdvancedOrderAnalytics,
        source_transform="testing.model.orders.transforms.adv_analytics.AdvancedOrderAnalytics",
        generated_package=generated_package,
        source_schema_modules=advanced_analytics.source_schema_modules(),
    )

    with generated_project(tmp_path, generated_package, files):
        schemas = advanced_analytics.generated_schemas(generated_package)
        inputs = advanced_analytics.input_frames(spark, schemas)
        generated = AdvancedOrderAnalytics(**inputs).run(
            session(spark, execution_mode="generated", generated_package=generated_package)
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
        assert grand_total["order_count"] == 4
        assert grand_total["large_order_count"] == 1
        assert grand_total["quantity_total"] == 20
        assert grand_total["any_large_order"] is True
        assert grand_total["all_large_orders"] is False

        cube_total = single(
            generated.product_cubes,
            lambda row: row["tenant_id"] is None and row["product_category"] is None and row["customer_tier"] is None,
        )
        assert cube_total["order_count"] == 4
        assert cube_total["distinct_customers"] == 3
        assert cube_total["gross_total"] == Decimal("2000.00")

        customer_second = single(
            generated.customer_windows,
            lambda row: row["customer_id"] == "c-1" and row["order_id"] == "o-3",
        )
        assert customer_second["percent_rank"] == 1.0
        assert customer_second["cume_dist"] == 1.0
        assert customer_second["second_order_id"] == "o-3"
        assert customer_second["running_units"] == 7
        assert customer_second["running_order_count"] == 2

        profile = single(generated.collection_profiles, lambda row: row["id"] == "o-1")
        assert profile["has_priority"] is True
        assert profile["tag_count"] == 3
        assert profile["contains_priority"] is True
        assert profile["contains_region"] is True
        assert profile["default_tags"] == ["priority", "standard"]
        assert profile["repeated_tags"] == ["priority", "priority"]
        assert profile["all_tags"] == ["priority", "new", "gift", "seasonal"]
        assert profile["tags_without_extra"] == ["new", "gift"]
        assert profile["first_tag"] == "priority"
        assert profile["safe_tag"] == "new"
        assert profile["region"] == "NA"
        assert profile["safe_region"] == "NA"
        assert profile["all_tags_present"] is True
        assert profile["score_total"] == 6
        assert profile["flat_tags"] == ["priority", "new", "gift"]
        attribute_keys = profile["attribute_keys"]
        assert isinstance(attribute_keys, list)
        assert sorted(attribute_keys) == ["Campaign", "Channel"]
        assert profile["roundtrip_attributes"] == {"Channel": "WEB", "Campaign": "SUMMER"}
        assert profile["merged_attributes"] == {"Channel": "WEB", "Campaign": "SUMMER", "Region": "NA"}

    assert_generated_connect_safe(files)
