from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class RecommendationRequest(Schema):
    tenant = struct(TenantKey, nullable=False)
    id = string(nullable=False)
    customer_id = string(nullable=True)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    category = string(nullable=True)
    collection_id = string(nullable=True)
    requested_at = timestamp(nullable=False)


class RecommendedProduct(Schema):
    tenant = struct(TenantKey, nullable=False)
    request_id = string(nullable=False)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    product_id = string(nullable=False)
    product_name = string(nullable=True)
    category = string(nullable=True)
    rank = long(nullable=False)
    base_score = double(nullable=False)
    promotion_score = double(nullable=False)
    boost_score = double(nullable=False)
    suppression_penalty = double(nullable=False)
    inventory_boost = double(nullable=False)
    feedback_score = double(nullable=False)
    final_score = double(nullable=False)
    feedback_contributed = boolean(nullable=False)


class RecommendationRun(Schema):
    tenant = struct(TenantKey, nullable=False)
    request_id = string(nullable=False)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    result_count = long(nullable=False)
    feedback_contributed = boolean(nullable=False)
