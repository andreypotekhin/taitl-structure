from structure import Decimal, String, Structure, field


class OrderRaw(Structure):
    id = field(String(), nullable=False, primary_key=True)
    customer_id = field(String(), nullable=False)
    total = field(String(), nullable=True)


class OrderNormalized(Structure):
    id = field(String(), nullable=False, primary_key=True)
    customer_id = field(String(), nullable=False)
    total = field(Decimal(12, 2), nullable=False)
