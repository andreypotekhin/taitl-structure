from examples.store.schemas.common import TenantKey
from examples.store.schemas.merchandising.evaluation import DailyRecommendationBehavior, RecommendationRequestBehavior
from examples.store.schemas.merchandising.feedback import ProductRecommendationSignal
from examples.store.schemas.merchandising.recommendation import RecommendedProduct
from structure import Schema
from structure.plugin.pyspark import *


class RankedRecommendationCandidate(RecommendedProduct):
    maximum_results = long(nullable=False)


class DiversifiedRecommendationCandidate(RankedRecommendationCandidate):
    diversity_rank = long(nullable=False)


class DiversificationDecision(Schema):
    tenant = struct(TenantKey, nullable=False)
    request_id = string(nullable=False)
    product_id = string(nullable=False)
    taxonomy_branch = string(nullable=True)
    branch_rank = long(nullable=False)
    selected = boolean(nullable=False)
    exclusion_reason = string(nullable=True)


class RecommendationBehaviorImpression(Schema):
    window = struct(TimeWindow, nullable=False)
    tenant = struct(TenantKey, nullable=False)
    request_id = string(nullable=False)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    impression_id = string(nullable=False)
    shown_at = timestamp(nullable=False)
    product_id = string(nullable=False)
    rank = long(nullable=False)
    examination_propensity = double(nullable=False)
    click_count = long(nullable=False)


class RecommendationClickSummary(RecommendationRequestBehavior):
    pass


class RecommendationExposure(Schema):
    window = struct(TimeWindow, nullable=False)
    tenant = struct(TenantKey, nullable=False)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    exposure_weight = double(nullable=False)
    click_weight = double(nullable=False)


class DailyRecommendationCounts(DailyRecommendationBehavior):
    pass


class ProductRecommendationSignalTotals(ProductRecommendationSignal):
    exposure_weight = double(nullable=False)
    click_weight = double(nullable=False)
