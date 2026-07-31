from examples.store.schemas.common import AuditStamp, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class CatalogProduct(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    product_id = string(nullable=False)
    product_name = string(nullable=True)
    category = string(nullable=True)
    active = boolean(nullable=False)
    list_price = decimal(12, 2, nullable=True)
    rating = double(nullable=True)
    has_promotion = boolean(nullable=False)
    promotion_code = string(nullable=True)
    promotion_name = string(nullable=True)
    promotion_discount = decimal(12, 2, nullable=True)
    base_score = double(nullable=False)
    promotion_score = double(nullable=False)
    eligible = boolean(nullable=False)


class CatalogAvailability(Schema):
    tenant = struct(TenantKey, nullable=False)
    product_id = string(nullable=False)
    available_to_promise = long(nullable=False)
    inventory_boost = double(nullable=False)
    availability_status = string(nullable=True)


class RecommendationCandidate(Schema):
    tenant = struct(TenantKey, nullable=False)
    request_id = string(nullable=False)
    requested_at = timestamp(nullable=False)
    customer_id = string(nullable=True)
    session_id = string(nullable=True)
    strategy_id = string(nullable=False)
    policy_version = string(nullable=False)
    experiment_id = string(nullable=True)
    experiment_version = string(nullable=True)
    variant_id = string(nullable=True)
    category_filter = string(nullable=True)
    collection_id = string(nullable=True)
    product_id = string(nullable=False)
    product_name = string(nullable=True)
    category = string(nullable=True)
    has_promotion = boolean(nullable=False)
    promotion_code = string(nullable=True)
    base_score = double(nullable=False)
    promotion_score = double(nullable=False)
    inventory_boost = double(nullable=False)
    candidate_source = string(nullable=False)
    taxonomy_id = string(nullable=True)
    taxonomy_branch = string(nullable=True)
    session_match = boolean(nullable=False)
    purchase_signal = double(nullable=False)
    eligibility_status = string(nullable=False)


class RecommendationCandidateDecision(Schema):
    tenant = struct(TenantKey, nullable=False)
    request_id = string(nullable=False)
    product_id = string(nullable=False)
    stage = string(nullable=False)
    eligible = boolean(nullable=False)
    exclusion_reason = string(nullable=True)
    candidate_source = string(nullable=False)
    taxonomy_branch = string(nullable=True)
