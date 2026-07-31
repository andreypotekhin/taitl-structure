from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class RecommendationEvaluationBatch(Schema):
    window = struct(TimeWindow, nullable=False)
    batch_id = string(nullable=False)


class RecommendationRequestBehavior(Schema):
    window = struct(TimeWindow, nullable=False)
    tenant = struct(TenantKey, nullable=False)
    request_id = string(nullable=False)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    result_count = long(nullable=False)
    clicked_result_count = long(nullable=False)
    has_click = boolean(nullable=True)
    first_click_rank = long(nullable=True)
    raw_click_count = long(nullable=False)


class DailyRecommendationBehavior(Schema):
    window = struct(TimeWindow, nullable=False)
    tenant = struct(TenantKey, nullable=False)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    request_count = long(nullable=False)
    zero_result_request_count = long(nullable=False)
    clicked_request_count = long(nullable=False)
    zero_result_rate = double(nullable=True)
    clicked_request_rate = double(nullable=True)
    mean_first_click_rank = double(nullable=True)
    raw_click_count = long(nullable=False)
    exposure_adjusted_click_rate = double(nullable=True)
