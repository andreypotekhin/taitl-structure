from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class RecommendationBehavior(Schema):
    window = struct(TimeWindow, nullable=False)
    tenant = struct(TenantKey, nullable=False)
    request_id = string(nullable=False)
    experiment_id = string(nullable=True)
    experiment_version = string(nullable=True)
    variant_id = string(nullable=True)
    result_count = long(nullable=False)
    has_click = boolean(nullable=False)
    attributed_purchase_count = long(nullable=False)


class RecommendationVariantMetric(Schema):
    window = struct(TimeWindow, nullable=False)
    tenant = struct(TenantKey, nullable=False)
    experiment_id = string(nullable=False)
    experiment_version = string(nullable=False)
    variant_id = string(nullable=False)
    request_count = long(nullable=False)
    zero_result_request_count = long(nullable=False)
    zero_result_rate = double(nullable=False)
    impression_count = long(nullable=False)
    clicked_request_count = long(nullable=False)
    click_through_rate = double(nullable=False)
    attributed_purchase_count = long(nullable=False)
    conversion_rate = double(nullable=False)
    maximum_zero_result_rate = double(nullable=True)
    zero_result_guardrail_met = boolean(nullable=True)


class RecommendationVariantMetricTotals(Schema):
    window = struct(TimeWindow, nullable=False)
    tenant = struct(TenantKey, nullable=False)
    experiment_id = string(nullable=False)
    experiment_version = string(nullable=False)
    variant_id = string(nullable=False)
    request_count = long(nullable=False)
    zero_result_request_count = long(nullable=False)
    impression_count = long(nullable=False)
    clicked_request_count = long(nullable=False)
    attributed_purchase_count = long(nullable=False)
    maximum_zero_result_rate = double(nullable=True)
