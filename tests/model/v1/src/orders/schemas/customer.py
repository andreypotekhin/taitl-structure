from structure import Schema, String, field

from orders.schemas.common import AuditStamp, TenantKey


class Customer(TenantKey, AuditStamp):
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    tier = field(String(), nullable=True)
    region = field(String(), nullable=True)
    email = field(String(), nullable=True)
