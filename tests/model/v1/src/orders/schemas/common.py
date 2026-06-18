from structure import Date, String, Structure, Timestamp, field


class TenantKey(Structure):
    tenant_id = field(String(), nullable=False, primary_key=True)


class AuditStamp(Structure):
    source_system = field(String(), nullable=False)
    ingested_at = field(Timestamp(), nullable=False)


class Address(Structure):
    line1 = field(String(), nullable=False)
    line2 = field(String(), nullable=True)
    city = field(String(), nullable=False)
    state = field(String(), nullable=True)
    postal_code = field(String(), nullable=False)
    country = field(String(), nullable=False)


class BusinessDate(Structure):
    order_date = field(Date(), nullable=True)
