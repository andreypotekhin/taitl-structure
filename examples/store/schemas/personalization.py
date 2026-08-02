"""Schemas for tenant-scoped personal recommendation signals."""

from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class UserFeaturePreference(Schema):
    """Explicit category preferences for one known customer in one tenant."""

    tenant = struct(TenantKey, nullable=False)
    customer_id = string(nullable=False)
    included_categories = array(string(), contains_null=False, nullable=False)
    excluded_categories = array(string(), contains_null=False, nullable=False)
    updated_at = timestamp(nullable=False)


class PersonalizationHistory(Schema):
    """Aggregated interaction strength for one customer/session product category."""

    tenant = struct(TenantKey, nullable=False)
    customer_id = string(nullable=True)
    session_id = string(nullable=True)
    product_id = string(nullable=False)
    category = string(nullable=True)
    history_score = double(nullable=False)


class PersonalizedRecommendation(Schema):
    """One request-scoped personal score ready for the main recommender."""

    tenant = struct(TenantKey, nullable=False)
    request_id = string(nullable=False)
    customer_id = string(nullable=True)
    session_id = string(nullable=True)
    product_id = string(nullable=False)
    feature_score = double(nullable=False)
    history_score = double(nullable=False)
    factorization_score = double(nullable=False)
    personal_score = double(nullable=False)
    matched_category = boolean(nullable=False)
    excluded_by_preference = boolean(nullable=False)
    algorithm_id = string(nullable=False)
    algorithm_version = string(nullable=False)
