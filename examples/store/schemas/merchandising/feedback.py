from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class RecommendationImpression(Schema):
    tenant = struct(TenantKey, nullable=False)
    id = string(nullable=False)
    request_id = string(nullable=False)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    product_id = string(nullable=False)
    rank = long(nullable=False)
    examination_propensity = double(nullable=False)
    customer_id = string(nullable=True)
    session_id = string(nullable=True)
    experiment_id = string(nullable=True)
    experiment_version = string(nullable=True)
    variant_id = string(nullable=True)
    shown_at = timestamp(nullable=False)


class RecommendationClick(Schema):
    id = string(nullable=False)
    impression_id = string(nullable=False)
    customer_id = string(nullable=True)
    occurred_at = timestamp(nullable=False)


class DailyRecommendationImpressions(Schema):
    window = struct(TimeWindow, nullable=False)
    tenant = struct(TenantKey, nullable=False)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    product_id = string(nullable=False)
    rank = long(nullable=False)
    examination_propensity = double(nullable=False)
    impression_count = long(nullable=False)


class DailyRecommendationClicks(Schema):
    window = struct(TimeWindow, nullable=False)
    tenant = struct(TenantKey, nullable=False)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    product_id = string(nullable=False)
    rank = long(nullable=False)
    examination_propensity = double(nullable=False)
    click_count = long(nullable=False)
    clicked_impression_count = long(nullable=False)


class ProductRecommendationSignal(Schema):
    tenant = struct(TenantKey, nullable=False)
    strategy_id = string(nullable=False)
    product_id = string(nullable=False)
    impression_count = long(nullable=False)
    clicked_impression_count = long(nullable=False)
    raw_click_count = long(nullable=False)
    click_through_rate = double(nullable=True)
    exposure_adjusted_click_rate = double(nullable=True)
    attributed_purchase_count = long(nullable=False)
    conversion_rate = double(nullable=True)


class RecommendationPurchase(Schema):
    tenant = struct(TenantKey, nullable=False)
    order_id = string(nullable=False)
    request_id = string(nullable=True)
    customer_id = string(nullable=True)
    session_id = string(nullable=True)
    product_id = string(nullable=False)
    recommendation_impression_id = string(nullable=True)
    occurred_at = timestamp(nullable=False)
    attribution_status = string(nullable=False)
    quantity = long(nullable=False)
