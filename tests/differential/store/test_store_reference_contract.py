from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, cast

from helpers.example_projects import render_store_example

from examples.store.transforms.evaluation.recommender.behavior.workflow import EvaluateRecommendations
from examples.store.transforms.fulfillment.demand import PrepareOrderDemand
from examples.store.transforms.fulfillment.workflow import Fulfillment
from examples.store.transforms.merchandising.clicks.build_signals import BuildRecommendationSignals
from examples.store.transforms.merchandising.ranking import Ranker
from examples.store.transforms.merchandising.recommender.admit import SelectRecommendationCandidates
from examples.store.transforms.merchandising.recommender.rank import RankRecommendationCandidates
from examples.store.transforms.merchandising.recommender.summarize import SummarizeRecommendationRuns
from examples.store.transforms.merchandising.recommender.workflow import Recommender
from examples.store.transforms.merchandising.workflow import Merchandising
from examples.store.transforms.order import EnrichOrders
from examples.store.transforms.rowset_joins.rowset_join_examples import RowsetJoinExamples
from structure.core.compiler.api import Compiler
from structure.plugin.api.v1.model.TransformPlan import TransformPlan
from structure.plugin.pyspark import literal
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan

Row = dict[str, Any]


def test_store_example_matches_independent_reference_rows() -> None:
    rows = _reference_enrich_orders(
        orders=[
            _order(" O-1 ", " C-1 ", " P-1 ", " SUMMER ", "1250.50", "10.00", 2),
            _order("bad", " C-1 ", "missing", "", "8.00", "0.00", None),
        ],
        customers=[{"tenant": {"tenant_id": "t1"}, "id": "c-1", "name": "Ada Lovelace", "tier": "gold"}],
        products=[{"tenant": {"tenant_id": "t1"}, "id": "p-1", "name": "Analytical Engine", "category": "compute"}],
        promotions=[{"tenant": {"tenant_id": "t1"}, "code": "summer", "name": "Summer"}],
    )

    assert rows == [
        {
            "tenant": {"tenant_id": "t1"},
            "business": {"order_date": "2026-01-02"},
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


def test_orders_generated_code_matches_independent_reference_operations() -> None:
    transform = render_store_example()["examples/structure_generated/store/pyspark/transforms/order.py"]

    reference_fragments = [
        'assert_schema(orders, ORDER_RAW_SCHEMA, name="OrderRaw", mode="strict")',
        'assert_schema(customers, CUSTOMER_SCHEMA, name="Customer", mode="strict")',
        'assert_schema(products, PRODUCT_SCHEMA, name="Product", mode="strict")',
        'assert_schema(promotions, PROMOTION_SCHEMA, name="Promotion", mode="strict")',
        'orders = orders.where(',
        'F.col("order_raw.id").isNotNull()',
        'F.coalesce(F.col("order_raw.total").cast("decimal(12,2)"), F.lit(0)).alias("total")',
        'customers_joined = F.broadcast(customers.alias("customers"))',
        'products_joined = products.alias("products")',
        'promotions_joined = promotions.alias("promotions")',
        'assert_schema(published, ORDER_PUBLISHED_SCHEMA, name="OrderPublished", mode="strict")',
    ]

    for fragment in reference_fragments:
        assert fragment in transform


def test_store_live_event_inputs_are_marked_streaming_at_source_boundaries() -> None:
    assert _input_modes(EnrichOrders)["orders"]
    assert _input_modes(PrepareOrderDemand)["orders"]
    assert _input_modes(Fulfillment)["orders"]
    assert _input_modes(BuildRecommendationSignals)["impressions"]
    assert _input_modes(BuildRecommendationSignals)["clicks"]

    for transform in (
        SelectRecommendationCandidates,
        Recommender,
        Merchandising,
        SummarizeRecommendationRuns,
    ):
        modes = _input_modes(transform)

        assert modes["requests"]
        assert any(not streaming for name, streaming in modes.items() if name != "requests")

    merchandising_modes = _input_modes(Merchandising)
    assert merchandising_modes["feedback_impressions"]
    assert merchandising_modes["feedback_clicks"]
    assert not merchandising_modes["evaluation_requests"]

    assert not _input_modes(EvaluateRecommendations)["requests"]
    assert not _input_modes(RowsetJoinExamples)["orders"]


def test_fulfillment_planning_matches_independent_reference_rows() -> None:
    rows = _reference_plan_fulfillment(
        demand=[
            _demand("o-allocated", "c-1", "east", "p-1", 3),
            _demand("o-partial", "c-2", "east", "p-2", 6),
            _demand("o-backorder", "c-3", "east", "p-3", 5),
        ],
        warehouses=[
            _warehouse("wh-east", "east", 1),
            _warehouse("wh-west", "west", 2),
        ],
        inventory_positions=[
            _inventory("wh-east", "p-1", 8, 0, 6),
            _inventory("wh-west", "p-1", 20, 0, 2),
            _inventory("wh-east", "p-2", 4, 1, 5),
            _inventory("wh-east", "p-3", 0, 0, 1),
        ],
        inbound_inventory=[
            _inbound("wh-east", "p-2", 20, "2026-01-05"),
        ],
    )

    assert rows["plans"] == [
        {
            "order_id": "o-allocated",
            "selected_warehouse_id": "wh-east",
            "allocated_quantity": 3,
            "backordered_quantity": 0,
            "planned_ship_date": "2026-01-02",
            "plan_status": "allocated",
        },
        {
            "order_id": "o-backorder",
            "selected_warehouse_id": None,
            "allocated_quantity": 0,
            "backordered_quantity": 5,
            "planned_ship_date": None,
            "plan_status": "backordered",
        },
        {
            "order_id": "o-partial",
            "selected_warehouse_id": "wh-east",
            "allocated_quantity": 3,
            "backordered_quantity": 3,
            "planned_ship_date": "2026-01-05",
            "plan_status": "partially_allocated",
        },
    ]
    assert rows["allocations"] == [
        {"order_id": "o-allocated", "warehouse_id": "wh-east", "allocated_quantity": 3},
        {"order_id": "o-partial", "warehouse_id": "wh-east", "allocated_quantity": 3},
    ]
    assert rows["backorders"] == [
        {"order_id": "o-backorder", "warehouse_id": None, "backordered_quantity": 5},
        {"order_id": "o-partial", "warehouse_id": "wh-east", "backordered_quantity": 3},
    ]
    assert rows["replenishment_suggestions"] == [
        {
            "warehouse_id": "wh-east",
            "product_id": "p-1",
            "available_to_promise_after_plan": 5,
            "safety_stock_quantity": 6,
            "earliest_inbound_at": None,
            "reason": "backorder_without_inbound",
        },
        {
            "warehouse_id": "wh-east",
            "product_id": "p-2",
            "available_to_promise_after_plan": 0,
            "safety_stock_quantity": 5,
            "earliest_inbound_at": "2026-01-05",
            "reason": "below_safety_stock_after_allocation",
        },
        {
            "warehouse_id": "wh-east",
            "product_id": "p-3",
            "available_to_promise_after_plan": 0,
            "safety_stock_quantity": 1,
            "earliest_inbound_at": None,
            "reason": "backorder_without_inbound",
        },
    ]


def test_fulfillment_generated_code_exposes_planning_contract() -> None:
    transform = render_store_example()["examples/structure_generated/store/pyspark/transforms/plan.py"]

    reference_fragments = [
        'assert_schema(demand, ORDER_SCHEMA, name="Order", mode="strict")',
        'assert_schema(warehouses, WAREHOUSE_SCHEMA, name="Warehouse", mode="strict")',
        'assert_schema(inventory_positions, INVENTORY_POSITION_SCHEMA, name="InventoryPosition", mode="strict")',
        'alias("available_to_promise")',
        'F.row_number()',
        'F.col("fulfillment_option.warehouse_priority").asc()',
        'F.col("fulfillment_option.available_to_promise").desc()',
        'F.col("fulfillment_option.warehouse_id").asc()',
        'alias("planned_ship_date")',
        "below_safety_stock_after_allocation",
    ]

    for fragment in reference_fragments:
        assert fragment in transform


def test_fulfillment_pipeline_generated_code_exposes_overall_flow() -> None:
    transform = render_store_example()["examples/structure_generated/store/pyspark/transforms/fulfillment_workflow.py"]

    reference_fragments = [
        "# Source: examples.store.transforms.fulfillment.workflow.Fulfillment",
        "class FulfillmentGenerated(",
        "# Step method: prepared.publish_demand",
        "# Step method: planned.plan",
        "# Step method: summarized.publish_daily_summary",
        'demand = frames["prepared__demand"]',
        'plans = frames["planned__plans"]',
        'daily_summary = frames["summarized__daily_summary"]',
    ]

    for fragment in reference_fragments:
        assert fragment in transform


def test_fulfillment_followup_generated_code_exposes_temporal_policy_and_service_contracts() -> None:
    generated = render_store_example()
    order = generated["examples/structure_generated/store/pyspark/transforms/order.py"]
    projection = generated["examples/structure_generated/store/pyspark/transforms/project_inventory.py"]
    substitution = generated["examples/structure_generated/store/pyspark/transforms/find.py"]
    exception = generated["examples/structure_generated/store/pyspark/transforms/exceptions.py"]
    service = generated["examples/structure_generated/store/pyspark/transforms/service.py"]

    assert '(F.col("shipments.line_number") == F.col("order_with_promotion.line_number"))' in order
    assert "F.date_add(F.col(\"demand_window.window_start\")" in projection
    assert "Window.partitionBy" in projection
    assert "F.row_number()" in substitution
    assert 'F.lit(\'substitution_available\')' in exception
    assert 'F.lit(\'service_target_at_risk\')' in exception
    assert 'F.lit(\'on_time_in_full\')' in service
    assert 'F.lit(\'late_in_full\')' in service
    assert 'F.lit(\'on_time_partial\')' in service
    assert 'F.lit(\'not_shipped\')' in service


def test_merchandising_recommendations_match_independent_reference_rows() -> None:
    rows = _reference_merchandise(
        requests=[
            _request("r-1", "strategy-a", "v1", "featured"),
        ],
        products=[
            _product("p-1", "Planner", "featured", active=True),
            _product("p-2", "Notebook", "featured", active=True),
            _product("p-3", "Retired", "featured", active=False),
            _product("p-4", "Blocked", "featured", active=True),
        ],
        blocked_products=[_product("p-4", "Blocked", "featured", active=True)],
        promotions=[{"tenant": {"tenant_id": "t1"}, "code": "featured", "name": "Featured"}],
        policy=[_policy("strategy-a", "v1")],
        boosts=[_boost("v1", product_id="p-2", boost_score=0.1)],
        suppressions=[],
        signals=[
            _signal("strategy-a", "p-1", impressions=40, clicks=8, ctr=0.2),
            _signal("strategy-a", "p-2", impressions=5, clicks=4, ctr=0.8),
        ],
    )

    assert rows["products"] == [
        {
            "request_id": "r-1",
            "product_id": "p-1",
            "rank": 1,
            "base_score": 1.0,
            "promotion_score": 0.5,
            "boost_score": 0.0,
            "suppression_penalty": 0.0,
            "inventory_boost": 0.0,
            "feedback_score": 0.2,
            "final_score": 1.7,
            "feedback_contributed": True,
        },
        {
            "request_id": "r-1",
            "product_id": "p-2",
            "rank": 2,
            "base_score": 1.0,
            "promotion_score": 0.5,
            "boost_score": 0.1,
            "suppression_penalty": 0.0,
            "inventory_boost": 0.0,
            "feedback_score": 0.0,
            "final_score": 1.6,
            "feedback_contributed": False,
        },
    ]
    assert rows["runs"] == [
        {
            "request_id": "r-1",
            "strategy_id": "strategy-a",
            "policy_version": "v1",
            "result_count": 2,
            "feedback_contributed": True,
        }
    ]


def test_recommend_generated_code_exposes_named_score_and_ranking_contract() -> None:
    transform = render_store_example()["examples/structure_generated/store/pyspark/transforms/recommender_workflow.py"]

    reference_fragments = [
        "class RankRecommendationCandidatesGenerated:",
        "class SelectRecommendedProductsGenerated:",
        "class SummarizeRecommendationRunsGenerated:",
        "selected__requests, RECOMMENDATION_CANDIDATE_SCHEMA",
        "RANKED_RECOMMENDATION_CANDIDATE_SCHEMA",
        'assert_schema(policy, MERCHANDISING_POLICY_SCHEMA, name="MerchandisingPolicy", mode="strict")',
        'alias("boost_score")',
        'alias("suppression_penalty")',
        'alias("feedback_score")',
        'alias("final_score")',
        "F.row_number()",
        'F.coalesce(F.col("suppressions_3.penalty"), F.lit(0.0)).asc()',
        'F.col("recommendation_candidate.inventory_boost").desc()',
        'F.col("recommendation_candidate.product_id").asc()',
        'assert_schema(recommended_products, RECOMMENDED_PRODUCT_SCHEMA, name="RecommendedProduct", mode="strict")',
    ]

    for fragment in reference_fragments:
        assert fragment in transform


def test_recommendation_ranker_formulas_are_swappable() -> None:
    assert getattr(Ranker, "_structure_special_type", None) == "expr"

    class ConstantBoostRanker(Ranker):
        def boost_score(self, boost: Any) -> Any:
            return literal(9.0)

    class ConstantBoostRankRecommendationCandidates(RankRecommendationCandidates):
        ranker = ConstantBoostRanker()

    plan = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(
            ConstantBoostRankRecommendationCandidates,
            materialize_schemas=False,
            target_profile=None,
        ).lowered,
    )
    boost_score = _projection(plan, "boost_score")

    assert boost_score.kind == "literal"
    assert boost_score.data["value"] == 9.0

    class ConstantBoostRecommender(Recommender):
        ranker = ConstantBoostRanker()

    recommender_plan = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(
            ConstantBoostRecommender,
            materialize_schemas=False,
            target_profile=None,
            allow_stream_to_batch=True,
        ).lowered,
    )
    recommender_boost_score = next(
        projection.expression
        for step in recommender_plan.steps
        for projection in step.projection
        if projection.field.name == "boost_score"
    )

    assert recommender_boost_score.kind == "literal"
    assert recommender_boost_score.data["value"] == 9.0


def test_merchandising_evaluation_keeps_zero_result_requests_by_strategy() -> None:
    rows = _reference_merchandising_behavior(
        batch={"start": "2026-01-02T00:00:00", "end": "2026-01-03T00:00:00"},
        requests=[
            _request("r-1", "strategy-a", "v1", None),
            _request("r-2", "strategy-a", "v1", None),
            _request("r-3", "strategy-b", "v2", None),
        ],
        impressions=[
            _impression("i-1", "r-1", "strategy-a", "v1", "p-1", 1, 0.5, "2026-01-02T01:00:00"),
            _impression("i-2", "r-1", "strategy-a", "v1", "p-2", 2, 1.0, "2026-01-02T01:00:01"),
            _impression("i-3", "r-3", "strategy-b", "v2", "p-3", 1, 0.5, "2026-01-02T02:00:00"),
        ],
        clicks=[
            {"id": "c-1", "impression_id": "i-2", "occurred_at": "2026-01-02T01:05:00"},
            {"id": "c-late", "impression_id": "i-1", "occurred_at": "2026-01-04T01:00:00"},
        ],
    )

    assert rows["requests"] == [
        {
            "request_id": "r-1",
            "strategy_id": "strategy-a",
            "policy_version": "v1",
            "result_count": 2,
            "clicked_result_count": 1,
            "has_click": True,
            "first_click_rank": 2,
            "raw_click_count": 1,
        },
        {
            "request_id": "r-2",
            "strategy_id": "strategy-a",
            "policy_version": "v1",
            "result_count": 0,
            "clicked_result_count": 0,
            "has_click": False,
            "first_click_rank": None,
            "raw_click_count": 0,
        },
        {
            "request_id": "r-3",
            "strategy_id": "strategy-b",
            "policy_version": "v2",
            "result_count": 1,
            "clicked_result_count": 0,
            "has_click": False,
            "first_click_rank": None,
            "raw_click_count": 0,
        },
    ]
    assert rows["daily"] == [
        {
            "strategy_id": "strategy-a",
            "policy_version": "v1",
            "request_count": 2,
            "zero_result_request_count": 1,
            "clicked_request_count": 1,
            "zero_result_rate": 0.5,
            "clicked_request_rate": 0.5,
            "mean_first_click_rank": 2.0,
            "raw_click_count": 1,
            "exposure_adjusted_click_rate": 0.333333,
        },
        {
            "strategy_id": "strategy-b",
            "policy_version": "v2",
            "request_count": 1,
            "zero_result_request_count": 0,
            "clicked_request_count": 0,
            "zero_result_rate": 0.0,
            "clicked_request_rate": 0.0,
            "mean_first_click_rank": None,
            "raw_click_count": 0,
            "exposure_adjusted_click_rate": 0.0,
        },
    ]


def _reference_enrich_orders(
    *,
    orders: list[Row],
    customers: list[Row],
    products: list[Row],
    promotions: list[Row],
) -> list[Row]:
    published = []
    for raw in orders:
        if not raw["id"] or not raw["customer_id"] or not raw["product_id"]:
            continue

        total = Decimal(str(raw["total"] or "0"))
        discount = Decimal(str(raw["discount"] or "0"))
        net_total = total - discount
        if net_total < 0:
            continue

        tenant = raw["tenant"]
        order = {
            "tenant": tenant,
            "business": raw["business"],
            "id": _clean(raw["id"]),
            "customer_id": _clean(raw["customer_id"]),
            "product_id": _clean(raw["product_id"]),
            "promotion_code": _clean(raw["promotion_code"]),
            "total": total,
            "discount": discount,
            "net_total": net_total,
            "quantity": raw["quantity"] or 1,
            "is_large": total > 1000,
        }
        customer = _find(customers, tenant=tenant, key="id", value=order["customer_id"], clean=True)
        product = _find(products, tenant=tenant, key="id", value=order["product_id"])
        if product is None:
            continue
        promotion = _find(promotions, tenant=tenant, key="code", value=order["promotion_code"], clean=True)

        published.append(
            {
                "tenant": tenant,
                "business": order["business"],
                "id": order["id"],
                "customer_id": order["customer_id"],
                "customer_name": customer["name"] if customer else None,
                "customer_tier": customer["tier"] if customer else None,
                "product_name": product["name"],
                "product_category": product["category"],
                "promotion_name": promotion["name"] if promotion else None,
                "total": total,
                "discount": discount,
                "net_total": net_total,
                "quantity": order["quantity"],
                "is_large": order["is_large"],
                "has_promotion": promotion is not None,
            }
        )
    return published


def _input_modes(transform: type) -> dict[str, bool]:
    # The store model intentionally mixes live feeds with batch-oriented stages.
    compilation = Compiler.frontend.compile()(
        transform,
        materialize_schemas=False,
        target_profile=None,
        allow_stream_to_batch=True,
    )
    plan = cast(TransformPlan, compilation.analysis)
    return {input.name: input.streaming for input in plan.inputs}


def _projection(plan: PySparkExecutionPlan, field_name: str) -> Any:
    for projection in plan.steps[0].projection:
        if projection.field.name == field_name:
            return projection.expression
    raise AssertionError(f"Projection {field_name} not found.")


def _reference_plan_fulfillment(
    *,
    demand: list[Row],
    warehouses: list[Row],
    inventory_positions: list[Row],
    inbound_inventory: list[Row],
) -> dict[str, list[Row]]:
    inbound_by_key = _inbound_availability(inbound_inventory)
    allocations = []
    backorders = []
    plans = []
    suggestions = []
    for line in demand:
        options = []
        for inventory in inventory_positions:
            warehouse = _find_warehouse(
                warehouses,
                tenant=inventory["tenant"],
                warehouse_id=inventory["warehouse_id"],
            )
            if warehouse is None or not warehouse["active"]:
                continue
            if inventory["tenant"] != line["tenant"] or inventory["product_id"] != line["product_id"]:
                continue
            available = max(int(inventory["on_hand_quantity"]) - int(inventory["reserved_quantity"]), 0)
            inbound = inbound_by_key.get((line["tenant"]["tenant_id"], warehouse["id"], line["product_id"]), {})
            options.append(
                {
                    "warehouse_id": warehouse["id"],
                    "warehouse_region": warehouse["region"],
                    "warehouse_priority": warehouse["priority"],
                    "available_to_promise": available,
                    "safety_stock_quantity": inventory["safety_stock_quantity"],
                    "earliest_inbound_at": inbound.get("earliest_expected_at"),
                }
            )
        if not options:
            continue
        option = min(
            options,
            key=lambda item: (
                0 if item["warehouse_region"] == line["customer_region"] else 1,
                item["warehouse_priority"],
                -item["available_to_promise"],
                item["warehouse_id"],
            ),
        )
        requested = int(line["requested_quantity"])
        available = int(option["available_to_promise"])
        allocated = min(available, requested) if available > 0 else 0
        backordered = requested - allocated
        planned_ship_date = line["business"]["order_date"] if backordered == 0 else option["earliest_inbound_at"]
        if allocated > 0:
            allocations.append(
                {
                    "order_id": line["order_id"],
                    "warehouse_id": option["warehouse_id"],
                    "allocated_quantity": allocated,
                }
            )
        if backordered > 0:
            backorders.append(
                {
                    "order_id": line["order_id"],
                    "warehouse_id": option["warehouse_id"] if allocated > 0 else None,
                    "backordered_quantity": backordered,
                }
            )
        plans.append(
            {
                "order_id": line["order_id"],
                "selected_warehouse_id": option["warehouse_id"] if allocated > 0 else None,
                "allocated_quantity": allocated,
                "backordered_quantity": backordered,
                "planned_ship_date": planned_ship_date,
                "plan_status": _plan_status(allocated, backordered),
            }
        )
        after_plan = available - allocated
        if after_plan < int(option["safety_stock_quantity"]) and (
            option["earliest_inbound_at"] is None or option["earliest_inbound_at"] > line["business"]["order_date"]
        ):
            suggestions.append(
                {
                    "warehouse_id": option["warehouse_id"],
                    "product_id": line["product_id"],
                    "available_to_promise_after_plan": after_plan,
                    "safety_stock_quantity": option["safety_stock_quantity"],
                    "earliest_inbound_at": option["earliest_inbound_at"],
                    "reason": (
                        "backorder_without_inbound"
                        if option["earliest_inbound_at"] is None
                        else "below_safety_stock_after_allocation"
                    ),
                }
            )
    return {
        "allocations": sorted(allocations, key=lambda row: row["order_id"]),
        "backorders": sorted(backorders, key=lambda row: row["order_id"]),
        "plans": sorted(plans, key=lambda row: row["order_id"]),
        "replenishment_suggestions": sorted(suggestions, key=lambda row: (row["warehouse_id"], row["product_id"])),
    }


def _reference_merchandise(
    *,
    requests: list[Row],
    products: list[Row],
    blocked_products: list[Row],
    promotions: list[Row],
    policy: list[Row],
    boosts: list[Row],
    suppressions: list[Row],
    signals: list[Row],
) -> dict[str, list[Row]]:
    catalog = []
    blocked = {(row["tenant"]["tenant_id"], row["id"]) for row in blocked_products}
    for product in products:
        if not product["active"] or (product["tenant"]["tenant_id"], product["id"]) in blocked:
            continue
        promotion = next(
            (
                item
                for item in promotions
                if item["tenant"] == product["tenant"]
                and _clean(item["code"]) in {_clean(product["id"]), _clean(product["category"])}
            ),
            None,
        )
        catalog.append(
            {
                "tenant": product["tenant"],
                "product_id": product["id"],
                "product_name": product["name"],
                "category": product["category"],
                "has_promotion": promotion is not None,
                "base_score": 1.0,
                "promotion_score": 0.5 if promotion else 0.0,
                "inventory_boost": 0.0,
            }
        )

    recommended = []
    runs = []
    for request in requests:
        version = _find(policy, tenant=request["tenant"], key="policy_version", value=request["policy_version"])
        if version is None:
            continue
        ranked = []
        for product in catalog:
            if product["tenant"] != request["tenant"]:
                continue
            if request["category"] is not None and request["category"] != product["category"]:
                continue
            boost_score = sum(
                item["boost_score"]
                for item in boosts
                if item["active"]
                and item["tenant"] == request["tenant"]
                and item["policy_version"] == request["policy_version"]
                and (item["product_id"] == product["product_id"] or item["category"] == product["category"])
            )
            matched_suppressions = [
                item
                for item in suppressions
                if item["active"]
                and item["tenant"] == request["tenant"]
                and item["policy_version"] == request["policy_version"]
                and (item["product_id"] == product["product_id"] or item["category"] == product["category"])
            ]
            if any(item["exclude"] for item in matched_suppressions):
                continue
            penalty = sum(item["penalty"] for item in matched_suppressions)
            signal = next(
                (
                    item
                    for item in signals
                    if item["tenant"] == request["tenant"]
                    and item["strategy_id"] == request["strategy_id"]
                    and item["product_id"] == product["product_id"]
                ),
                None,
            )
            feedback_contributed = False
            feedback_score = 0.0
            if signal is not None and signal["impression_count"] >= version["minimum_feedback_impressions"]:
                feedback_contributed = True
                feedback_score = signal["click_through_rate"] * version["feedback_weight"]
            final_score = (
                product["base_score"]
                + product["promotion_score"]
                + boost_score
                + product["inventory_boost"]
                + feedback_score
                - penalty
            )
            ranked.append(
                {
                    "request_id": request["id"],
                    "product_id": product["product_id"],
                    "rank": 0,
                    "base_score": product["base_score"],
                    "promotion_score": product["promotion_score"],
                    "boost_score": boost_score,
                    "suppression_penalty": penalty,
                    "inventory_boost": product["inventory_boost"],
                    "feedback_score": feedback_score,
                    "final_score": round(final_score, 6),
                    "feedback_contributed": feedback_contributed,
                }
            )
        ranked.sort(
            key=lambda row: (
                -row["final_score"],
                row["suppression_penalty"],
                -row["inventory_boost"],
                row["product_id"],
            )
        )
        ranked = ranked[: version["maximum_results"]]
        for index, row in enumerate(ranked, start=1):
            row["rank"] = index
        recommended.extend(ranked)
        runs.append(
            {
                "request_id": request["id"],
                "strategy_id": request["strategy_id"],
                "policy_version": request["policy_version"],
                "result_count": len(ranked),
                "feedback_contributed": any(row["feedback_contributed"] for row in ranked),
            }
        )
    return {
        "products": sorted(recommended, key=lambda row: (row["request_id"], row["rank"])),
        "runs": sorted(runs, key=lambda row: row["request_id"]),
    }


def _reference_merchandising_behavior(
    *,
    batch: Row,
    requests: list[Row],
    impressions: list[Row],
    clicks: list[Row],
) -> dict[str, list[Row]]:
    start = _ts(batch["start"])
    end = _ts(batch["end"])
    selected = [request for request in requests if start <= _ts(request["requested_at"]) < end]
    timely_clicks = {
        impression["id"]: [
            click
            for click in clicks
            if click["impression_id"] == impression["id"]
            and _ts(impression["shown_at"])
            <= _ts(click["occurred_at"])
            <= _ts(impression["shown_at"]) + timedelta(hours=24)
        ]
        for impression in impressions
    }
    request_rows = []
    for request in selected:
        displayed = [
            item for item in impressions if item["tenant"] == request["tenant"] and item["request_id"] == request["id"]
        ]
        clicked = [item for item in displayed if timely_clicks[item["id"]]]
        request_rows.append(
            {
                "request_id": request["id"],
                "strategy_id": request["strategy_id"],
                "policy_version": request["policy_version"],
                "result_count": len(displayed),
                "clicked_result_count": len(clicked),
                "has_click": bool(clicked),
                "first_click_rank": min((item["rank"] for item in clicked), default=None),
                "raw_click_count": sum(len(timely_clicks[item["id"]]) for item in displayed),
            }
        )

    daily = []
    keys = sorted({(row["strategy_id"], row["policy_version"]) for row in request_rows})
    for strategy_id, policy_version in keys:
        rows = [
            row for row in request_rows if row["strategy_id"] == strategy_id and row["policy_version"] == policy_version
        ]
        shown = [
            item
            for item in impressions
            if item["strategy_id"] == strategy_id
            and item["policy_version"] == policy_version
            and item["request_id"] in {row["request_id"] for row in rows}
        ]
        exposure = sum(1.0 / item["examination_propensity"] for item in shown)
        click_weight = sum((1.0 / item["examination_propensity"]) for item in shown if timely_clicks[item["id"]])
        clicked_ranks = [row["first_click_rank"] for row in rows if row["first_click_rank"] is not None]
        daily.append(
            {
                "strategy_id": strategy_id,
                "policy_version": policy_version,
                "request_count": len(rows),
                "zero_result_request_count": sum(1 for row in rows if row["result_count"] == 0),
                "clicked_request_count": sum(1 for row in rows if row["has_click"]),
                "zero_result_rate": round(sum(1 for row in rows if row["result_count"] == 0) / len(rows), 6),
                "clicked_request_rate": round(sum(1 for row in rows if row["has_click"]) / len(rows), 6),
                "mean_first_click_rank": (round(sum(clicked_ranks) / len(clicked_ranks), 6) if clicked_ranks else None),
                "raw_click_count": sum(row["raw_click_count"] for row in rows),
                "exposure_adjusted_click_rate": round(click_weight / exposure, 6) if exposure else None,
            }
        )
    return {
        "requests": sorted(request_rows, key=lambda row: row["request_id"]),
        "daily": daily,
    }


def _order(
    id: str,
    customer_id: str,
    product_id: str,
    promotion_code: str,
    total: str,
    discount: str,
    quantity: int | None,
    line_number: int = 1,
) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "business": {"order_date": "2026-01-02"},
        "id": id,
        "line_number": line_number,
        "customer_id": customer_id,
        "product_id": product_id,
        "promotion_code": promotion_code,
        "total": total,
        "discount": discount,
        "quantity": quantity,
    }


def _request(id: str, strategy_id: str, policy_version: str, category: str | None) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "id": id,
        "customer_id": "c-1",
        "strategy_id": strategy_id,
        "policy_version": policy_version,
        "category": category,
        "collection_id": None,
        "requested_at": "2026-01-02T01:00:00",
    }


def _product(id: str, name: str, category: str, *, active: bool) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "id": id,
        "name": name,
        "category": category,
        "active": active,
    }


def _policy(strategy_id: str, policy_version: str) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "strategy_id": strategy_id,
        "policy_version": policy_version,
        "maximum_results": 10,
        "minimum_feedback_impressions": 20,
        "feedback_weight": 1.0,
    }


def _boost(
    policy_version: str,
    *,
    product_id: str | None = None,
    category: str | None = None,
    boost_score: float,
) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "policy_version": policy_version,
        "product_id": product_id,
        "category": category,
        "boost_score": boost_score,
        "active": True,
    }


def _signal(strategy_id: str, product_id: str, *, impressions: int, clicks: int, ctr: float) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "strategy_id": strategy_id,
        "product_id": product_id,
        "impression_count": impressions,
        "clicked_impression_count": clicks,
        "raw_click_count": clicks,
        "click_through_rate": ctr,
    }


def _impression(
    id: str,
    request_id: str,
    strategy_id: str,
    policy_version: str,
    product_id: str,
    rank: int,
    examination_propensity: float,
    shown_at: str,
) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "id": id,
        "request_id": request_id,
        "strategy_id": strategy_id,
        "policy_version": policy_version,
        "product_id": product_id,
        "rank": rank,
        "examination_propensity": examination_propensity,
        "shown_at": shown_at,
    }


def _demand(
    order_id: str,
    customer_id: str,
    customer_region: str,
    product_id: str,
    requested_quantity: int,
) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "business": {"order_date": "2026-01-02"},
        "order_id": order_id,
        "customer_id": customer_id,
        "customer_region": customer_region,
        "product_id": product_id,
        "requested_quantity": requested_quantity,
    }


def _warehouse(warehouse_id: str, region: str, priority: int) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "id": warehouse_id,
        "region": region,
        "priority": priority,
        "active": True,
    }


def _inventory(
    warehouse_id: str,
    product_id: str,
    on_hand_quantity: int,
    reserved_quantity: int,
    safety_stock_quantity: int,
) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "warehouse_id": warehouse_id,
        "product_id": product_id,
        "on_hand_quantity": on_hand_quantity,
        "reserved_quantity": reserved_quantity,
        "safety_stock_quantity": safety_stock_quantity,
    }


def _inbound(
    warehouse_id: str,
    product_id: str,
    expected_quantity: int,
    expected_at: str,
) -> Row:
    return {
        "tenant": {"tenant_id": "t1"},
        "warehouse_id": warehouse_id,
        "product_id": product_id,
        "expected_quantity": expected_quantity,
        "expected_at": expected_at,
    }


def _inbound_availability(rows: list[Row]) -> dict[tuple[str, str, str], Row]:
    availability: dict[tuple[str, str, str], Row] = {}
    for row in rows:
        key = (row["tenant"]["tenant_id"], row["warehouse_id"], row["product_id"])
        current = availability.get(key)
        if current is None:
            availability[key] = {
                "earliest_expected_at": row["expected_at"],
                "expected_quantity": row["expected_quantity"],
            }
            continue
        current["earliest_expected_at"] = min(current["earliest_expected_at"], row["expected_at"])
        current["expected_quantity"] = int(current["expected_quantity"]) + int(row["expected_quantity"])
    return availability


def _find_warehouse(
    warehouses: list[Row],
    *,
    tenant: object,
    warehouse_id: object,
) -> Row | None:
    for warehouse in warehouses:
        if warehouse["tenant"] == tenant and warehouse["id"] == warehouse_id:
            return warehouse
    return None


def _plan_status(allocated: int, backordered: int) -> str:
    if backordered == 0:
        return "allocated"
    if allocated > 0:
        return "partially_allocated"
    return "backordered"


def _clean(value: object) -> str:
    return str(value).strip().lower()


def _ts(value: object) -> datetime:
    return datetime.fromisoformat(str(value))


def _find(
    rows: list[Row],
    *,
    tenant: object,
    key: str,
    value: object,
    clean: bool = False,
) -> Row | None:
    for row in rows:
        candidate = _clean(row[key]) if clean else row[key]
        if row["tenant"] == tenant and candidate == value:
            return row
    return None
