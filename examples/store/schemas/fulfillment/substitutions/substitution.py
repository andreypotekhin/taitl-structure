from examples.store.schemas.common import AuditStamp, TenantKey
from structure import Schema
from structure.plugin.pyspark import *


class SubstitutionRule(Schema):
    tenant = struct(TenantKey, nullable=False)
    audit = struct(AuditStamp, nullable=False)
    product_id = string(nullable=False)
    substitute_product_id = string(nullable=False)
    equivalence_group = string(nullable=False)
    policy_rank = integer(nullable=False)
    active = boolean(nullable=False)


class FulfillmentSubstitutionOption(Schema):
    tenant = struct(TenantKey, nullable=False)
    order_id = string(nullable=False)
    line_number = integer(nullable=False)
    customer_id = string(nullable=False)
    original_product_id = string(nullable=False)
    substitute_product_id = string(nullable=False)
    equivalence_group = string(nullable=False)
    policy_rank = integer(nullable=False)
    available_to_promise = long(nullable=False)
    option_rank = long(nullable=False)
    reason = string(nullable=False)
