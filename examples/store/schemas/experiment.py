from examples.store.schemas.common import TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class RecommendationExperiment(Schema):
    tenant = struct(TenantKey, nullable=False)
    experiment_id = string(nullable=False)
    experiment_version = string(nullable=False)
    name = string(nullable=False)
    variant_a = string(nullable=False)
    variant_b = string(nullable=False)
    variant_a_percent = long(nullable=False)
    active = boolean(nullable=False)
    maximum_zero_result_rate = double(nullable=True)


class RecommendationAssignment(Schema):
    tenant = struct(TenantKey, nullable=False)
    experiment_id = string(nullable=False)
    experiment_version = string(nullable=False)
    assignment_key = string(nullable=False)
    variant_id = string(nullable=False)
    assigned_at = timestamp(nullable=False)


class RecommendationExposure(Schema):
    tenant = struct(TenantKey, nullable=False)
    request_id = string(nullable=False)
    experiment_id = string(nullable=False)
    experiment_version = string(nullable=False)
    variant_id = string(nullable=False)
    exposed_at = timestamp(nullable=False)
