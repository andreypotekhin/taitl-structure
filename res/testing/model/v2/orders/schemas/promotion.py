from testing.model.v2.orders.schemas.common import AuditStamp, TenantKey

from structure import *


class Promotion(Schema):
    tenant = field(Struct(TenantKey), nullable=False)
    audit = field(Struct(AuditStamp), nullable=False)
    code = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    discount = field(Decimal(12, 2), nullable=True)
    valid_from = field(Date(), nullable=False)
    valid_to = field(Date(), nullable=True)
