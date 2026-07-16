from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest
from integration.pyspark.support.backend_matrix import (
    assert_generated_connect_safe,
    backend_name,
    generated_project,
    render_generated_project,
    session,
)
from integration.pyspark.support.backend_matrix import spark as _spark
from integration.pyspark.support.rows import rows, single
from integration.pyspark.v2.support import advanced_analytics

from structure.lib.testing import assert_online_generated_parity

# These public scenarios require the opt-in live backend harness.
spark_fixture = pytest.fixture(name="spark")(getattr(_spark, "__wrapped__"))
pytestmark = pytest.mark.integration


def test_scalar_expression_concept_has_live_online_generated_parity(spark, tmp_path) -> None:
    _requires_pyspark4("V3 scalar concept uses try_cast(...)")
    V3OrderFeatures, source_schema_modules = _scalar_source()
    package = "concept_v3_scalar_generated"
    files = render_generated_project(
        V3OrderFeatures,
        source_transform="testing.model.v3.orders.transforms.v3.V3OrderFeatures",
        generated_package=package,
        source_schema_modules=source_schema_modules,
    )

    with generated_project(tmp_path, package, files):
        schemas = __import__(f"{package}.pyspark.schemas.v3", fromlist=["V3_ORDER_SOURCE_SCHEMA"])
        frame = spark.createDataFrame(
            [
                ("o-1", "order-1", "12", Decimal("10.25"), 1.0, None, None, ("external-1", "west")),
                ("o-2", None, None, None, None, None, None, (None, None)),
            ],
            schemas.V3_ORDER_SOURCE_SCHEMA,
        )

        def online():
            return V3OrderFeatures(orders=frame).run(session(spark, execution_mode="online"))

        def generated():
            return V3OrderFeatures(orders=frame).run(
                session(spark, execution_mode="generated", generated_package=package)
            )

        assert_online_generated_parity(online, generated, outputs=("projected",), ordered=True)
        projected = single(generated().projected, lambda row: row["id"] == "o-1")
        assert projected["quantity"] == 12
        assert projected["display_name"] == "order-1 · west"
        assert projected["absolute_amount"] == Decimal("10.25")
        assert projected["recency_rank"] == 1

    assert_generated_connect_safe(files)


def test_join_concept_has_live_online_generated_parity(spark, tmp_path) -> None:
    from integration.pyspark.v2.support import rowset_joins

    RowsetJoinExamples = rowset_joins.transform()
    package = "concept_v3_joins_generated"
    files = render_generated_project(
        RowsetJoinExamples,
        source_transform="testing.model.v2.orders.transforms.rowset_join.RowsetJoinExamples",
        generated_package=package,
        source_schema_modules=rowset_joins.source_schema_modules(),
    )

    with generated_project(tmp_path, package, files):
        schemas = rowset_joins.generated_schemas(package)
        inputs = rowset_joins.input_frames(spark, schemas)

        def online():
            return RowsetJoinExamples(**inputs).run(session(spark, execution_mode="online"))

        def generated():
            return RowsetJoinExamples(**inputs).run(
                session(spark, execution_mode="generated", generated_package=package)
            )

        assert_online_generated_parity(online, generated, outputs=("candidates",))
        assert len(rows(generated().candidates, "customer_id", "product_id")) == 4

    assert_generated_connect_safe(files)


@pytest.mark.parametrize(
    ("concept", "output", "assertion"),
    [
        (
            "aggregates",
            "revenue_rollups",
            lambda frame: single(
                frame,
                lambda row: row["tenant_id"] is None and row["product_category"] is None and row["order_date"] is None,
            )["quantity_total"]
            == 20,
        ),
        (
            "windows",
            "customer_windows",
            lambda frame: single(
                frame,
                lambda row: row["customer_id"] == "c-1" and row["order_id"] == "o-3",
            )["running_units"]
            == 7,
        ),
        (
            "collections",
            "collection_profiles",
            lambda frame: single(frame, lambda row: row["id"] == "o-1")["score_total"] == 6,
        ),
    ],
    ids=("aggregates", "windows", "collections"),
)
def test_analytical_concept_has_live_online_generated_parity(
    spark,
    tmp_path,
    concept: str,
    output: str,
    assertion: Callable[[Any], bool],
) -> None:
    if concept == "collections":
        _requires_pyspark4("V3 collection concept uses try_element_at(...)")
    AdvancedOrderAnalytics, source_schema_modules = _analytics_source()
    package = f"concept_v3_{concept}_generated"
    files = render_generated_project(
        AdvancedOrderAnalytics,
        source_transform="testing.model.v3.orders.transforms.adv_analytics.AdvancedOrderAnalytics",
        generated_package=package,
        source_schema_modules=source_schema_modules,
    )

    with generated_project(tmp_path, package, files):
        schemas = advanced_analytics.generated_schemas(package)
        inputs = advanced_analytics.input_frames(spark, schemas)

        def online():
            return AdvancedOrderAnalytics(**inputs).run(session(spark, execution_mode="online"))

        def generated():
            return AdvancedOrderAnalytics(**inputs).run(
                session(spark, execution_mode="generated", generated_package=package)
            )

        assert_online_generated_parity(online, generated, outputs=(output,))
        assert assertion(getattr(generated(), output))

    assert_generated_connect_safe(files)


def test_source_ordered_step_and_filter_concept_has_live_online_generated_parity(spark, tmp_path) -> None:
    _requires_pyspark4("V3 step/filter concept uses try_cast(...)")
    V3OrderFeatures, source_schema_modules = _scalar_source()
    package = "concept_v3_step_filter_generated"
    files = render_generated_project(
        V3OrderFeatures,
        source_transform="testing.model.v3.orders.transforms.v3.V3OrderFeatures",
        generated_package=package,
        source_schema_modules=source_schema_modules,
    )

    with generated_project(tmp_path, package, files):
        schemas = __import__(f"{package}.pyspark.schemas.v3", fromlist=["V3_ORDER_SOURCE_SCHEMA"])
        frame = spark.createDataFrame(
            [
                ("kept", "order-2", "4", Decimal("1.00"), 0.0, None, None, ("external-2", "east")),
                ("filtered", None, "4", Decimal("1.00"), 0.0, None, None, ("external-3", "east")),
                ("filtered-too", "order-3", None, Decimal("1.00"), 0.0, None, None, ("external-4", "east")),
            ],
            schemas.V3_ORDER_SOURCE_SCHEMA,
        )

        def online():
            return V3OrderFeatures(orders=frame).run(session(spark, execution_mode="online"))

        def generated():
            return V3OrderFeatures(orders=frame).run(
                session(spark, execution_mode="generated", generated_package=package)
            )

        assert_online_generated_parity(online, generated, outputs=("projected",))
        assert [row["id"] for row in rows(generated().projected, "id")] == ["kept"]

    assert_generated_connect_safe(files)


def _scalar_source():
    from testing.model.v3.orders.schemas.v3 import V3OrderDetails, V3OrderProjection, V3OrderSource
    from testing.model.v3.orders.transforms.v3 import V3OrderFeatures

    return V3OrderFeatures, {
        "testing.model.v3.orders.schemas.v3": [V3OrderDetails, V3OrderProjection, V3OrderSource],
    }


def _analytics_source():
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


def _requires_pyspark4(feature: str) -> None:
    if backend_name().endswith("35"):
        pytest.skip(f"{feature} is available in Structure's PySpark 4.0 capability profile only")
