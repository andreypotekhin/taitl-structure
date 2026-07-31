from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class MerchandisingPolicy(Schema):
    tenant = struct(TenantKey, nullable=False)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    maximum_results = long(nullable=False)
    minimum_feedback_impressions = long(nullable=False)
    feedback_weight = double(nullable=False)


class MerchandisingBoost(Schema):
    tenant = struct(TenantKey, nullable=False)
    policy_version = string(nullable=False)
    product_id = string(nullable=True)
    category = string(nullable=True)
    boost_score = double(nullable=False)
    active = boolean(nullable=False)


class MerchandisingSuppression(Schema):
    tenant = struct(TenantKey, nullable=False)
    policy_version = string(nullable=False)
    product_id = string(nullable=True)
    category = string(nullable=True)
    penalty = double(nullable=False)
    exclude = boolean(nullable=False)
    active = boolean(nullable=False)
    reason = string(nullable=True)
