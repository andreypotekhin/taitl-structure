from structure import String, Struct, Structure, field

from orders.schemas.common import AuditStamp, TenantKey


class Customer(Structure):
    tenant = field(Struct(TenantKey), nullable=False)
    audit = field(Struct(AuditStamp), nullable=False)
    id = field(String(), nullable=False, primary_key=True)
    name = field(String(), nullable=True)
    tier = field(String(), nullable=True)
    region = field(String(), nullable=True)
    email = field(String(), nullable=True)
