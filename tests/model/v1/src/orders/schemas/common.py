from structure import Date, Schema, String, Timestamp, field


class TenantKey(Schema):
    tenant_id = field(String(), nullable=False, primary_key=True)


class AuditStamp(Schema):
    source_system = field(String(), nullable=False)
    ingested_at = field(Timestamp(), nullable=False)


class Address(Schema):
    line1 = field(String(), nullable=False)
    line2 = field(String(), nullable=True)
    city = field(String(), nullable=False)
    state = field(String(), nullable=True)
    postal_code = field(String(), nullable=False)
    country = field(String(), nullable=False)


class BusinessDate(Schema):
    order_date = field(Date(), nullable=True)
